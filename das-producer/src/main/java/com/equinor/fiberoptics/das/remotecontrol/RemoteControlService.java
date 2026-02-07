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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
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

/**
 * Applies and stops remote-controlled acquisitions.
 */
@Service
public class RemoteControlService {

  private static final Logger _logger = LoggerFactory.getLogger(RemoteControlService.class);

  private final BeanFactory _beanFactory;
  private final DasProducerConfiguration _dasProducerConfiguration;
  private final SimulatorBoxUnitConfiguration _simulatorBoxUnitConfiguration;
  private final DasProducerFactory _dasProducerFactory;
  private final KafkaRelay _kafkaRelay;
  private final KafkaSender _kafkaSender;
  private final ObjectMapper _objectMapper;
  private final AcquisitionProfileResolver _acquisitionProfileResolver;

  private final AtomicReference<Disposable> _runDisposable = new AtomicReference<>();
  private final AtomicReference<String> _currentConfigHash = new AtomicReference<>();
  private final AtomicReference<String> _lastAppliedAcquisitionId = new AtomicReference<>();

  /**
   * Creates the remote-control service with required dependencies.
   */
    @SuppressFBWarnings(
      value = "EI_EXPOSE_REP2",
      justification = "Spring-managed dependencies are intentionally shared."
    )
  public RemoteControlService(
      BeanFactory beanFactory,
      DasProducerConfiguration dasProducerConfiguration,
      SimulatorBoxUnitConfiguration simulatorBoxUnitConfiguration,
      DasProducerFactory dasProducerFactory,
      KafkaRelay kafkaRelay,
      KafkaSender kafkaSender,
      ObjectMapper objectMapper,
      AcquisitionProfileResolver acquisitionProfileResolver) {
    _beanFactory = beanFactory;
    _dasProducerConfiguration = dasProducerConfiguration;
    _simulatorBoxUnitConfiguration = simulatorBoxUnitConfiguration;
    _dasProducerFactory = dasProducerFactory;
    _kafkaRelay = kafkaRelay;
    _kafkaSender = kafkaSender;
    _objectMapper = objectMapper;
    _acquisitionProfileResolver = acquisitionProfileResolver;
  }

  /**
   * Result of a stop request.
   */
  public enum StopResult {
    STOPPED,
    ALREADY_STOPPED,
    NOT_FOUND
  }

  /**
   * Applies an acquisition configuration from JSON.
   */
  public synchronized void apply(String acquisitionJson) {
    if (acquisitionJson == null || acquisitionJson.isBlank()) {
      throw new BadRequestException(
        "Request body must be a DASAcquisition JSON payload."
      );
    }

    JsonNode root;
    try {
      root = _objectMapper.readTree(acquisitionJson);
    } catch (JsonProcessingException e) {
      throw new BadRequestException("Invalid JSON payload.");
    }

    JsonNode custom = root.get("Custom");
    if (custom == null) {
      custom = root.get("custom");
    }
    String profileJson = _acquisitionProfileResolver.resolveAcquisitionJson(custom);

    JsonNode profileRoot;
    try {
      profileRoot = _objectMapper.readTree(profileJson);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
        "Resolved profile JSON could not be parsed: " + e.getMessage(),
        e
      );
    }

    String hash = profileHash(profileRoot);
    boolean running = isRunning();
    if (running && hash.equals(_currentConfigHash.get())) {
      _logger.info("APPLY received with identical configuration while running. No-op.");
      return;
    }

    if (running) {
      String previousAcquisitionId = _lastAppliedAcquisitionId.get();
      if (previousAcquisitionId != null && !previousAcquisitionId.isBlank()) {
        try {
          _dasProducerFactory.stopAcquisitionBestEffort(previousAcquisitionId);
        } catch (Exception e) {
          _logger.warn(
              "Best-effort stop acquisition threw. Continuing with switch. {}",
              e.getMessage()
          );
        }
      }
    }

    ObjectNode updatedProfileRoot = withNewAcquisitionIdentity(profileRoot);
    String profileJsonWithNewIdentity;
    try {
      profileJsonWithNewIdentity = _objectMapper.writeValueAsString(updatedProfileRoot);
    } catch (Exception e) {
      throw new IllegalStateException(
        "Failed to serialize updated profile JSON: " + e.getMessage(),
        e
      );
    }

    applyAcquisitionToSimulatorConfiguration(updatedProfileRoot);

    stopCurrentInternal();

    optionalText(updatedProfileRoot, "AcquisitionId").ifPresent(_lastAppliedAcquisitionId::set);
    optionalText(updatedProfileRoot, "acquisitionId").ifPresent(_lastAppliedAcquisitionId::set);

    _kafkaSender.setProducers(
        _dasProducerFactory.createProducersFromAcquisitionJson(profileJsonWithNewIdentity)
    );

    String variant = Objects.requireNonNull(_dasProducerConfiguration.getVariant(), "variant");
    GenericDasProducer simulator = _beanFactory.getBean(
      variant,
      GenericDasProducer.class
    );
    Disposable disposable = simulator.produce()
        .subscribe(
            batch -> {
              for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry : batch) {
                _kafkaRelay.relayToKafka(entry);
              }
            },
            ex -> {
              _logger.warn("Error emitted from producer: {}", ex.getMessage(), ex);
              stopCurrentInternal();
            },
            () -> {
              _logger.info("Producer completed.");
              stopCurrentInternal();
            }
        );

    _runDisposable.set(disposable);
    _currentConfigHash.set(hash);

    _logger.info("APPLY accepted. Producer started.");
  }

  /**
   * Stops the current acquisition, optionally matching the requested id.
   */
  public synchronized StopResult stop(Optional<String> acquisitionJson) {
    Optional<String> requestedAcquisitionId = acquisitionJson
        .flatMap(json -> extractStringField(json, "AcquisitionId", "acquisitionId"));

    if (!isRunning()) {
      if (requestedAcquisitionId.isPresent()) {
        String last = _lastAppliedAcquisitionId.get();
        return requestedAcquisitionId.get().equals(last)
          ? StopResult.ALREADY_STOPPED
          : StopResult.NOT_FOUND;
      }
      return StopResult.ALREADY_STOPPED;
    }

    if (requestedAcquisitionId.isPresent()) {
      String last = _lastAppliedAcquisitionId.get();
      if (last != null && !requestedAcquisitionId.get().equals(last)) {
        return StopResult.NOT_FOUND;
      }
    }

    String stopAcquisitionId = requestedAcquisitionId.orElseGet(_lastAppliedAcquisitionId::get);
    if (stopAcquisitionId != null && !stopAcquisitionId.isBlank()) {
      try {
        _dasProducerFactory.stopAcquisitionBestEffort(stopAcquisitionId);
      } catch (Exception e) {
        _logger.warn(
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
    Disposable d = _runDisposable.get();
    return d != null && !d.isDisposed();
  }

  private void stopCurrentInternal() {
    Disposable d = _runDisposable.getAndSet(null);
    if (d != null && !d.isDisposed()) {
      try {
        d.dispose();
      } catch (Exception e) {
        _logger.warn("Exception disposing producer subscription: {}", e.getMessage());
      }
    }
    try {
      _kafkaRelay.teardown();
    } catch (Exception e) {
      _logger.warn("Exception tearing down kafka relay: {}", e.getMessage());
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

    _simulatorBoxUnitConfiguration.setNumberOfLoci(numberOfLoci);
    _simulatorBoxUnitConfiguration.setStartLocusIndex(startLocusIndex);

    optionalDouble(root, "PulseRate")
        .ifPresent(v -> _simulatorBoxUnitConfiguration.setPulseRate((int) Math.round(v)));
    optionalDouble(root, "MaximumFrequency")
        .ifPresent(v -> _simulatorBoxUnitConfiguration.setMaxFreq((float) v.doubleValue()));
    optionalDouble(root, "SpatialSamplingInterval")
        .ifPresent(
            v -> _simulatorBoxUnitConfiguration.setSpatialSamplingInterval((float) v.doubleValue())
        );
    optionalDouble(root, "GaugeLength")
        .ifPresent(v -> _simulatorBoxUnitConfiguration.setGaugeLength((float) v.doubleValue()));
    optionalDouble(root, "PulseWidth")
        .ifPresent(v -> _simulatorBoxUnitConfiguration.setPulseWidth((float) v.doubleValue()));

    optionalText(root, "OpticalPathUUID")
        .ifPresent(_simulatorBoxUnitConfiguration::setOpticalPathUUID);
    optionalText(root, "DasInstrumentBoxUUID")
        .ifPresent(_simulatorBoxUnitConfiguration::setBoxUUID);
    optionalText(root, "VendorCode")
        .ifPresent(_dasProducerConfiguration::setVendorCode);
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
      JsonNode root = _objectMapper.readTree(json);
      for (String name : candidateFieldNames) {
        JsonNode node = root.get(name);
        if (node != null && node.isTextual() && !node.asText().isBlank()) {
          return Optional.of(node.asText());
        }
      }
      return Optional.empty();
    } catch (JsonProcessingException e) {
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
    } catch (NoSuchAlgorithmException e) {
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
      byte[] canonical = _objectMapper.writer()
          .with(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .writeValueAsBytes(copy);
      return sha256Bytes(canonical);
    } catch (JsonProcessingException e) {
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
    } catch (NoSuchAlgorithmException e) {
      return Integer.toString(Arrays.hashCode(bytes));
    }
  }

  /**
   * Thrown when a request payload is invalid.
   */
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  public static class BadRequestException extends RuntimeException {
    public BadRequestException(String message) {
      super(message);
    }
  }

  /**
   * Thrown when remote-control credentials are invalid.
   */
  @ResponseStatus(HttpStatus.UNAUTHORIZED)
  public static class UnauthorizedException extends RuntimeException {
  }
}
