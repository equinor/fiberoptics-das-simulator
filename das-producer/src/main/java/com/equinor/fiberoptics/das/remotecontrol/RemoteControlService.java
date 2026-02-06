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

package com.equinor.fiberoptics.das.remotecontrol;

import com.equinor.fiberoptics.das.DasProducerFactory;
import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.GenericDasProducer;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnitConfiguration;
import com.equinor.fiberoptics.das.remotecontrol.profile.AcquisitionProfileResolver;
import com.equinor.kafka.KafkaRelay;
import com.equinor.kafka.KafkaSender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.ResponseStatus;
import reactor.core.Disposable;

@Service
public class RemoteControlService {

  private static final Logger logger = LoggerFactory.getLogger(RemoteControlService.class);

  private final BeanFactory beanFactory;
  private final DasProducerConfiguration dasProducerConfiguration;
  private final SimulatorBoxUnitConfiguration simulatorBoxUnitConfiguration;
  private final DasProducerFactory dasProducerFactory;
  private final KafkaRelay kafkaRelay;
  private final KafkaSender kafkaSender;
  private final ObjectMapper objectMapper;
  private final AcquisitionProfileResolver acquisitionProfileResolver;

  private final AtomicReference<Disposable> runDisposable = new AtomicReference<>();
  private final AtomicReference<String> currentConfigHash = new AtomicReference<>();
  private final AtomicReference<String> lastAppliedAcquisitionId = new AtomicReference<>();

  public RemoteControlService(
      BeanFactory beanFactory,
      DasProducerConfiguration dasProducerConfiguration,
      SimulatorBoxUnitConfiguration simulatorBoxUnitConfiguration,
      DasProducerFactory dasProducerFactory,
      KafkaRelay kafkaRelay,
      KafkaSender kafkaSender,
      ObjectMapper objectMapper,
      AcquisitionProfileResolver acquisitionProfileResolver) {
    this.beanFactory = beanFactory;
    this.dasProducerConfiguration = dasProducerConfiguration;
    this.simulatorBoxUnitConfiguration = simulatorBoxUnitConfiguration;
    this.dasProducerFactory = dasProducerFactory;
    this.kafkaRelay = kafkaRelay;
    this.kafkaSender = kafkaSender;
    this.objectMapper = objectMapper;
    this.acquisitionProfileResolver = acquisitionProfileResolver;
  }

  public enum StopResult {
    STOPPED,
    ALREADY_STOPPED,
    NOT_FOUND
  }

  public synchronized void apply(String acquisitionJson) {
    if (acquisitionJson == null || acquisitionJson.isBlank()) {
      throw new BadRequestException(
        "Request body must be a DASAcquisition JSON payload."
      );
    }

    JsonNode root;
    try {
      root = objectMapper.readTree(acquisitionJson);
    } catch (Exception e) {
      throw new BadRequestException("Invalid JSON payload.");
    }

    JsonNode custom = root.get("Custom");
    if (custom == null) {
      custom = root.get("custom");
    }
    String profileJson = acquisitionProfileResolver.resolveAcquisitionJson(custom);

    JsonNode profileRoot;
    try {
      profileRoot = objectMapper.readTree(profileJson);
    } catch (Exception e) {
      throw new IllegalStateException(
        "Resolved profile JSON could not be parsed: " + e.getMessage(),
        e
      );
    }

    String hash = profileHash(profileRoot);
    boolean running = isRunning();
    if (running && hash.equals(currentConfigHash.get())) {
      logger.info("APPLY received with identical configuration while running. No-op.");
      return;
    }

    if (running) {
      String previousAcquisitionId = lastAppliedAcquisitionId.get();
      if (previousAcquisitionId != null && !previousAcquisitionId.isBlank()) {
        try {
          dasProducerFactory.stopAcquisitionBestEffort(previousAcquisitionId);
        } catch (Exception e) {
          logger.warn(
              "Best-effort stop acquisition threw. Continuing with switch. {}",
              e.getMessage()
          );
        }
      }
    }

    ObjectNode updatedProfileRoot = withNewAcquisitionIdentity(profileRoot);
    String profileJsonWithNewIdentity;
    try {
      profileJsonWithNewIdentity = objectMapper.writeValueAsString(updatedProfileRoot);
    } catch (Exception e) {
      throw new IllegalStateException(
        "Failed to serialize updated profile JSON: " + e.getMessage(),
        e
      );
    }

    applyAcquisitionToSimulatorConfiguration(updatedProfileRoot);

    stopCurrentInternal();

    optionalText(updatedProfileRoot, "AcquisitionId").ifPresent(lastAppliedAcquisitionId::set);
    optionalText(updatedProfileRoot, "acquisitionId").ifPresent(lastAppliedAcquisitionId::set);

    kafkaSender.setProducers(
        dasProducerFactory.createProducersFromAcquisitionJson(profileJsonWithNewIdentity)
    );

    GenericDasProducer simulator = beanFactory.getBean(
        dasProducerConfiguration.getVariant(),
        GenericDasProducer.class
    );
    Disposable disposable = simulator.produce()
        .subscribe(
            batch -> {
              for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry : batch) {
                kafkaRelay.relayToKafka(entry);
              }
            },
            ex -> {
              logger.warn("Error emitted from producer: {}", ex.getMessage(), ex);
              stopCurrentInternal();
            },
            () -> {
              logger.info("Producer completed.");
              stopCurrentInternal();
            }
        );

    runDisposable.set(disposable);
    currentConfigHash.set(hash);

    logger.info("APPLY accepted. Producer started.");
  }

  public synchronized StopResult stop(Optional<String> acquisitionJson) {
    Optional<String> requestedAcquisitionId = acquisitionJson
        .flatMap(json -> extractStringField(json, "AcquisitionId", "acquisitionId"));

    if (!isRunning()) {
      if (requestedAcquisitionId.isPresent()) {
        String last = lastAppliedAcquisitionId.get();
        return requestedAcquisitionId.get().equals(last)
          ? StopResult.ALREADY_STOPPED
          : StopResult.NOT_FOUND;
      }
      return StopResult.ALREADY_STOPPED;
    }

    if (requestedAcquisitionId.isPresent()) {
      String last = lastAppliedAcquisitionId.get();
      if (last != null && !requestedAcquisitionId.get().equals(last)) {
        return StopResult.NOT_FOUND;
      }
    }

    String stopAcquisitionId = requestedAcquisitionId.orElseGet(lastAppliedAcquisitionId::get);
    if (stopAcquisitionId != null && !stopAcquisitionId.isBlank()) {
      try {
        dasProducerFactory.stopAcquisitionBestEffort(stopAcquisitionId);
      } catch (Exception e) {
        logger.warn(
            "Best-effort stop acquisition failed for {}: {}",
            stopAcquisitionId,
            e.getMessage()
        );
      }
    }
    stopCurrentInternal();
    return StopResult.STOPPED;
  }

  private boolean isRunning() {
    Disposable d = runDisposable.get();
    return d != null && !d.isDisposed();
  }

  private void stopCurrentInternal() {
    Disposable d = runDisposable.getAndSet(null);
    if (d != null && !d.isDisposed()) {
      try {
        d.dispose();
      } catch (Exception e) {
        logger.warn("Exception disposing producer subscription: {}", e.getMessage());
      }
    }
    try {
      kafkaRelay.teardown();
    } catch (Exception e) {
      logger.warn("Exception tearing down kafka relay: {}", e.getMessage());
    }
  }

  private void applyAcquisitionToSimulatorConfiguration(JsonNode root) {
    Integer numberOfLoci = requiredInt(root, "NumberOfLoci");
    Integer startLocusIndex = requiredInt(root, "StartLocusIndex");
    if (numberOfLoci <= 0) {
      throw new BadRequestException("NumberOfLoci must be > 0.");
    }
    if (startLocusIndex < 0) {
      throw new BadRequestException("StartLocusIndex must be >= 0.");
    }

    simulatorBoxUnitConfiguration.setNumberOfLoci(numberOfLoci);
    simulatorBoxUnitConfiguration.setStartLocusIndex(startLocusIndex);

    optionalDouble(root, "PulseRate")
        .ifPresent(v -> simulatorBoxUnitConfiguration.setPulseRate((int) Math.round(v)));
    optionalDouble(root, "MaximumFrequency")
        .ifPresent(v -> simulatorBoxUnitConfiguration.setMaxFreq((float) v.doubleValue()));
    optionalDouble(root, "SpatialSamplingInterval")
        .ifPresent(
            v -> simulatorBoxUnitConfiguration.setSpatialSamplingInterval((float) v.doubleValue())
        );
    optionalDouble(root, "GaugeLength")
        .ifPresent(v -> simulatorBoxUnitConfiguration.setGaugeLength((float) v.doubleValue()));
    optionalDouble(root, "PulseWidth")
        .ifPresent(v -> simulatorBoxUnitConfiguration.setPulseWidth((float) v.doubleValue()));

    optionalText(root, "OpticalPathUUID")
        .ifPresent(simulatorBoxUnitConfiguration::setOpticalPathUUID);
    optionalText(root, "DasInstrumentBoxUUID")
        .ifPresent(simulatorBoxUnitConfiguration::setBoxUUID);
    optionalText(root, "VendorCode")
        .ifPresent(dasProducerConfiguration::setVendorCode);
  }

  private ObjectNode withNewAcquisitionIdentity(JsonNode root) {
    if (!(root instanceof ObjectNode)) {
      throw new BadRequestException("Profile JSON must be an object.");
    }
    ObjectNode mutable = ((ObjectNode) root).deepCopy();
    String newAcquisitionId = UUID.randomUUID().toString();
    String newMeasurementStartTime = Instant.now().toString();
    replaceIdentityField(mutable, "AcquisitionId", "acquisitionId", newAcquisitionId);
    replaceIdentityField(
        mutable,
        "MeasurementStartTime",
        "measurementStartTime",
        newMeasurementStartTime
    );
    return mutable;
  }

  private void replaceIdentityField(
      ObjectNode node,
      String primary,
      String secondary,
      String value) {
    if (node.has(primary)) {
      node.put(primary, value);
      return;
    }
    if (node.has(secondary)) {
      node.put(secondary, value);
      return;
    }
    node.put(primary, value);
  }

  private Integer requiredInt(JsonNode root, String fieldName) {
    JsonNode node = root.get(fieldName);
    if (node == null || !node.canConvertToInt()) {
      throw new BadRequestException(
        fieldName + " is required and must be an integer."
      );
    }
    return node.asInt();
  }

  private Optional<Double> optionalDouble(JsonNode root, String fieldName) {
    JsonNode node = root.get(fieldName);
    if (node == null || !node.isNumber()) {
      return Optional.empty();
    }
    return Optional.of(node.asDouble());
  }

  private Optional<String> optionalText(JsonNode root, String fieldName) {
    JsonNode node = root.get(fieldName);
    if (node == null || !node.isTextual() || node.asText().isBlank()) {
      return Optional.empty();
    }
    return Optional.of(node.asText());
  }

  private Optional<String> extractStringField(String json, String... candidateFieldNames) {
    try {
      JsonNode root = objectMapper.readTree(json);
      for (String name : candidateFieldNames) {
        JsonNode node = root.get(name);
        if (node != null && node.isTextual() && !node.asText().isBlank()) {
          return Optional.of(node.asText());
        }
      }
      return Optional.empty();
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private Optional<Integer> extractIntField(String json, String... candidateFieldNames) {
    try {
      JsonNode root = objectMapper.readTree(json);
      for (String name : candidateFieldNames) {
        JsonNode node = root.get(name);
        if (node != null && node.canConvertToInt()) {
          return Optional.of(node.asInt());
        }
      }
      return Optional.empty();
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private String sha256(String s) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(hash.length * 2);
      for (byte b : hash) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (Exception e) {
      return Integer.toString(s.hashCode());
    }
  }

  private String profileHash(JsonNode root) {
    try {
      JsonNode copy = root.deepCopy();
      if (copy instanceof ObjectNode objectNode) {
        objectNode.remove("AcquisitionId");
        objectNode.remove("acquisitionId");
        objectNode.remove("MeasurementStartTime");
        objectNode.remove("measurementStartTime");
      }
      byte[] canonical = objectMapper.writer()
        .with(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
        .writeValueAsBytes(copy);
      return sha256Bytes(canonical);
    } catch (Exception e) {
      return sha256(root.toString());
    }
  }

  private String sha256Bytes(byte[] bytes) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(bytes);
      StringBuilder sb = new StringBuilder(hash.length * 2);
      for (byte b : hash) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (Exception e) {
      return Integer.toString(Arrays.hashCode(bytes));
    }
  }

  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public static class BadRequestException extends RuntimeException {
    public BadRequestException(String message) {
      super(message);
    }
  }

  @ResponseStatus(HttpStatus.UNAUTHORIZED)
  public static class UnauthorizedException extends RuntimeException {
  }
}
