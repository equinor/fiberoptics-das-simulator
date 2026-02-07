/*-
 * ========================LICENSE_START=================================
 * fiberoptics-das-producer
 * %%
 * Copyright (C) 2020 Equinor ASA
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

package com.equinor.fiberoptics.das;

import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.dto.AcquisitionStartDto;
import com.equinor.fiberoptics.das.error.ErrorCodeException;
import com.equinor.kafka.KafkaConfiguration;
import com.equinor.kafka.KafkaSender;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * Factory for creating Kafka producers based on acquisition payloads.
 */
@Component
public class DasProducerFactory {
  private static final Logger _logger = LoggerFactory.getLogger(DasProducerFactory.class);

  private final HttpUtils _httpUtils;
  private final KafkaConfiguration _configuration;
  private final DasProducerConfiguration _dasProducerConfig;
  private final KafkaSender _kafkaSender;

  private final ApplicationContext _applicationContext;
  private final AtomicReference<String> _lastAcquisitionId = new AtomicReference<>();

  DasProducerFactory(
      KafkaConfiguration kafkaConfig,
      HttpUtils http,
      DasProducerConfiguration dasProducerConfig,
      KafkaSender kafkaSender,
      ApplicationContext applicationContext) {
    _configuration = kafkaConfig;
    _httpUtils = http;
    _dasProducerConfig = dasProducerConfig;
    _kafkaSender = kafkaSender;
    _applicationContext = applicationContext;
  }

  /**
   * Logs shutdown when the Spring context is destroyed.
   */
  @PreDestroy
  public void onDestroy() throws Exception {
    _logger.info("Spring Container is destroyed!");
    Duration sleep = defaultIfNull(_dasProducerConfig.getShutdownSleep(), Duration.ofSeconds(1));
    Thread.sleep(Math.max(1L, sleep.toMillis()));
  }

  /**
   * Provides a producer bean for non-remote control mode.
   */
  @Bean
  @ConditionalOnProperty(
      prefix = "das.producer.remote-control",
      name = "enabled",
      havingValue = "false",
      matchIfMissing = true)
  public KafkaProducer<DASMeasurementKey, DASMeasurement> producerFactory() {
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers =
        createProducersFromAcquisitionJsonInternal(null, true);
    _kafkaSender.setProducers(producers);
    return producers.get(0);
  }

  /**
   * Creates a single producer from an acquisition JSON payload.
   */
  public KafkaProducer<DASMeasurementKey, DASMeasurement> createProducerFromAcquisitionJson(
      String acquisitionJson
  ) {
    return createProducerFromAcquisitionJsonInternal(acquisitionJson, false);
  }

  /**
   * Creates producer instances from an acquisition JSON payload.
   */
  public List<KafkaProducer<DASMeasurementKey, DASMeasurement>> createProducersFromAcquisitionJson(
      String acquisitionJson
  ) {
    return createProducersFromAcquisitionJsonInternal(acquisitionJson, false);
  }

  /**
   * Returns the last acquisition id observed during preflight.
   */
  public String getLastAcquisitionId() {
    return _lastAcquisitionId.get();
  }

  /**
   * Attempts to stop the acquisition, ignoring best-effort failures.
   */
  public void stopAcquisitionBestEffort(String acquisitionId) {
    if (acquisitionId == null || acquisitionId.isBlank()) {
      return;
    }
    try {
      _httpUtils.stopAcquisition(acquisitionId);
    } catch (Exception e) {
      _logger.warn(
          "Best-effort stop acquisition failed for {}: {}",
          acquisitionId,
          e.getMessage()
      );
    }
  }

  private KafkaProducer<DASMeasurementKey, DASMeasurement>
      createProducerFromAcquisitionJsonInternal(
          String acquisitionJson,
          boolean exitOnInvalidPartitions) {
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers =
        createProducersFromAcquisitionJsonInternal(acquisitionJson, exitOnInvalidPartitions);
    return producers.get(0);
  }

    @SuppressFBWarnings(
      value = "DM_EXIT",
      justification = "Legacy CLI behavior exits on invalid partitions in non-remote mode."
    )
    private List<KafkaProducer<DASMeasurementKey, DASMeasurement>>
      createProducersFromAcquisitionJsonInternal(
          String acquisitionJson,
          boolean exitOnInvalidPartitions) {
    String effectiveAcquisitionJson = acquisitionJson;
    if (effectiveAcquisitionJson == null || effectiveAcquisitionJson.isBlank()) {
      HttpUtils.SchemaVersions version = HttpUtils.SchemaVersions.valueOf(
          _dasProducerConfig.getAcquisitionStartVersion()
      );
      effectiveAcquisitionJson = version == HttpUtils.SchemaVersions.V1
          ? _httpUtils.asV1Json()
          : _httpUtils.asV2Json();
    }
    extractAcquisitionId(effectiveAcquisitionJson).ifPresent(_lastAcquisitionId::set);

    AcquisitionStartDto acquisition = startAcquisitionWithPreflight(effectiveAcquisitionJson);
    _logger.info(
        "Got TOPIC={}, BOOTSTRAP_SERVERS={}, SCHEMA_REGISTRY={}, NUMBER_OF_PARTITIONS={}",
        acquisition.getTopic(),
        acquisition.getBootstrapServers(),
        acquisition.getSchemaRegistryUrl(),
        acquisition.getNumberOfPartitions()
    );
    if (acquisition.getNumberOfPartitions() <= 0) {
      if (exitOnInvalidPartitions) {
        _logger.error(
            "We are unable to run when the destination topic has {} partitions. Exiting.",
            acquisition.getNumberOfPartitions()
        );
        _logger.info("Stopping");
        int exitValue = SpringApplication.exit(_applicationContext);
        System.exit(exitValue);
      }
        throw new ErrorCodeException(
          "APP-004",
          HttpStatus.INTERNAL_SERVER_ERROR,
          "Unable to run when the destination topic has "
            + acquisition.getNumberOfPartitions()
            + " partitions."
        );
    }

    String actualSchemaRegistryServers;
    if (_dasProducerConfig.getOverrideSchemaRegistryWith() != null
        && !_dasProducerConfig.getOverrideSchemaRegistryWith().isBlank()) {
      actualSchemaRegistryServers = _dasProducerConfig.getOverrideSchemaRegistryWith();
      _logger.info(
          "Overriding incoming schema registry server {} with: {}",
          acquisition.getSchemaRegistryUrl(),
          actualSchemaRegistryServers
      );
    } else {
      actualSchemaRegistryServers = acquisition.getSchemaRegistryUrl();
    }
    _logger.info("Preflight checking if Schema registry is alive.");
    waitForServiceOrTimeout(
        actualSchemaRegistryServers,
        () -> _httpUtils.checkIfServiceIsFine(actualSchemaRegistryServers),
        "Schema registry",
        "schema registry"
    );
    _dasProducerConfig.setPartitionAssignments(acquisition.getPartitionAssignments());
    _configuration.setTopic(acquisition.getTopic());
    _configuration.setPartitions(acquisition.getNumberOfPartitions());
    String actualBootstrapServeras;
    if (_dasProducerConfig.getOverrideBootstrapServersWith() != null
        && !_dasProducerConfig.getOverrideBootstrapServersWith().isBlank()) {
      actualBootstrapServeras = _dasProducerConfig.getOverrideBootstrapServersWith();
      _logger.info(
          "Overriding incoming bootstrap server {} with: {}",
          acquisition.getBootstrapServers(),
          actualBootstrapServeras
      );
    } else {
      actualBootstrapServeras = acquisition.getBootstrapServers();
    }
    Map<String, Object> baseProps = _configuration.kafkaProperties(
        actualBootstrapServeras,
        actualSchemaRegistryServers
    );
    int configuredInstances = Math.max(1, _configuration.getProducerInstances());
    int partitionsInAssignment = 0;
    Map<Integer, Integer> assignments = acquisition.getPartitionAssignments();
    if (assignments != null && !assignments.isEmpty()) {
      partitionsInAssignment = new HashSet<>(assignments.values()).size();
    }
    int effectivePartitionCount = Math.max(1, partitionsInAssignment);
    int instances = Math.min(configuredInstances, effectivePartitionCount);
    if (instances < configuredInstances) {
      _logger.info(
          "Reducing Kafka producer instances from {} to {} "
              + "(partition assignments cover {} partitions).",
          configuredInstances,
          instances,
          effectivePartitionCount
      );
    }
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers = new ArrayList<>(instances);
    for (int i = 0; i < instances; i++) {
      Map<String, Object> props = baseProps;
      if (instances > 1) {
        props = new HashMap<>(baseProps);
        Object clientId = props.get(ProducerConfig.CLIENT_ID_CONFIG);
        String baseClientId = clientId == null ? "das-simulator" : String.valueOf(clientId);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, baseClientId + "-" + i);
      }
      producers.add(new KafkaProducer<>(props));
    }
    return List.copyOf(producers);
  }

  private AcquisitionStartDto startAcquisitionWithPreflight(String acquisitionJson) {
    _logger.info("Preflight checking if initiator service is alive.");
    String healthCheckSi = _dasProducerConfig.getInitiatorserviceUrl().trim()
        + "/actuator/health";
    waitForServiceOrTimeout(
        healthCheckSi,
        () -> _httpUtils.checkIfServiceIsFine(healthCheckSi),
        "Stream initiator",
        "initiator service"
    );

    _logger.info(
        "Calling start acquisition on URL {}",
        _dasProducerConfig.getInitiatorserviceUrl().trim()
    );
    return _httpUtils.startAcquisition(acquisitionJson);
  }

  private Optional<String> extractAcquisitionId(String acquisitionJson) {
    try {
      JsonObject obj = JsonParser.parseString(acquisitionJson).getAsJsonObject();
      if (obj.has("AcquisitionId")) {
        return Optional.ofNullable(obj.get("AcquisitionId")).map(e -> e.getAsString());
      }
      if (obj.has("acquisitionId")) {
        return Optional.ofNullable(obj.get("acquisitionId")).map(e -> e.getAsString());
      }
    } catch (Exception e) {
      _logger.warn(
          "Unable to extract AcquisitionId from acquisition JSON: {}",
          e.getMessage()
      );
    }
    return Optional.empty();
  }

  private void waitForServiceOrTimeout(
      String url,
      BooleanSupplier isHealthy,
      String serviceDisplayName,
      String environmentComponentName) {
    Duration timeout = defaultIfNull(
      _dasProducerConfig.getHealthCheckTimeout(),
      Duration.ofMinutes(5)
    );
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    Duration interval = defaultIfNull(
      _dasProducerConfig.getHealthCheckInterval(),
      Duration.ofSeconds(5)
    );
    while (!isHealthy.getAsBoolean()) {
      if (System.nanoTime() > deadlineNanos) {
      throw new ErrorCodeException(
        "APP-002",
        HttpStatus.INTERNAL_SERVER_ERROR,
        serviceDisplayName
          + " did not become healthy within " + timeout + ". "
          + "This indicates an environment configuration error. "
          + "Last attempted URL: "
          + url
      );
      }
      _logger.info(
          "Trying to reach {} at {} looking for a 200 OK.",
          environmentComponentName,
          url
      );
      try {
        Thread.sleep(Math.max(1L, interval.toMillis()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ErrorCodeException(
            "APP-003",
            HttpStatus.INTERNAL_SERVER_ERROR,
            "Interrupted while waiting for " + serviceDisplayName + " to become healthy.",
            e
        );
      }
    }
  }

  private static Duration defaultIfNull(Duration value, Duration fallback) {
    return value == null ? fallback : value;
  }
}
