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

package com.equinor;

import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnitConfiguration;
import com.equinor.fiberoptics.das.producer.variants.staticdataunit.StaticDataUnitConfiguration;
import com.equinor.kafka.KafkaConfiguration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class StartupInfoLogger {

  private static final Logger logger = LoggerFactory.getLogger(StartupInfoLogger.class);

  private final Environment environment;
  private final ObjectProvider<BuildProperties> buildPropertiesProvider;
  private final DasProducerConfiguration dasProducerConfiguration;
  private final ObjectProvider<SimulatorBoxUnitConfiguration> simulatorBoxUnitConfigurationProvider;
  private final ObjectProvider<StaticDataUnitConfiguration> staticDataUnitConfigurationProvider;
  private final ObjectProvider<KafkaConfiguration> kafkaConfigurationProvider;

  public StartupInfoLogger(
      Environment environment,
      ObjectProvider<BuildProperties> buildPropertiesProvider,
      DasProducerConfiguration dasProducerConfiguration,
      ObjectProvider<SimulatorBoxUnitConfiguration> simulatorBoxUnitConfigurationProvider,
      ObjectProvider<StaticDataUnitConfiguration> staticDataUnitConfigurationProvider,
      ObjectProvider<KafkaConfiguration> kafkaConfigurationProvider
  ) {
    this.environment = environment;
    this.buildPropertiesProvider = buildPropertiesProvider;
    this.dasProducerConfiguration = dasProducerConfiguration;
    this.simulatorBoxUnitConfigurationProvider = simulatorBoxUnitConfigurationProvider;
    this.staticDataUnitConfigurationProvider = staticDataUnitConfigurationProvider;
    this.kafkaConfigurationProvider = kafkaConfigurationProvider;
  }

  @EventListener
  public void onApplicationReady(ApplicationReadyEvent ignored) {
    String message = buildStartupMessage();
    logger.info(message);
  }

  private String buildStartupMessage() {
    Map<String, Object> lines = new LinkedHashMap<>();
    lines.put("time", Instant.now().toString());
    lines.put("java", System.getProperty("java.version"));
    lines.put("profiles.active", join(environment.getActiveProfiles()));

    buildPropertiesProvider.ifAvailable(build -> {
      lines.put("app.group", build.getGroup());
      lines.put("app.artifact", build.getArtifact());
      lines.put("app.name", build.getName());
      lines.put("app.version", build.getVersion());
      lines.put("app.buildTime", build.getTime());
    });

    String implementationVersion = Optional.ofNullable(StartupInfoLogger.class.getPackage())
        .map(Package::getImplementationVersion)
        .orElse(null);
    if (implementationVersion != null && !implementationVersion.isBlank()) {
      lines.putIfAbsent("app.implementationVersion", implementationVersion);
    }

    lines.put("das.producer.variant", nullToEmpty(dasProducerConfiguration.getVariant()));
    lines.put("das.producer.vendorCode", nullToEmpty(dasProducerConfiguration.getVendorCode()));
    lines.put(
        "das.producer.amplitudesPrPackage",
        dasProducerConfiguration.getAmplitudesPrPackage()
    );
    lines.put(
        "das.producer.initiatorserviceUrl",
        nullToEmpty(dasProducerConfiguration.getInitiatorserviceUrl())
    );
    lines.put(
        "das.producer.acquisitionStartVersion",
        nullToEmpty(dasProducerConfiguration.getAcquisitionStartVersion())
    );
    lines.put(
        "das.producer.overrideBootstrapServersWith",
        nullToEmpty(dasProducerConfiguration.getOverrideBootstrapServersWith())
    );
    lines.put(
        "das.producer.overrideSchemaRegistryWith",
        nullToEmpty(dasProducerConfiguration.getOverrideSchemaRegistryWith())
    );

    if (dasProducerConfiguration.getRemoteControl() != null) {
      lines.put(
          "das.producer.remoteControl.enabled",
          dasProducerConfiguration.getRemoteControl().isEnabled()
      );
      lines.put(
          "das.producer.remoteControl.profilesDirectory",
          nullToEmpty(dasProducerConfiguration.getRemoteControl().getProfilesDirectory())
      );
      lines.put(
          "das.producer.remoteControl.apiKey",
          masked("REMOTE_CONTROL_API_KEY", dasProducerConfiguration.getRemoteControl().getApiKey())
      );
    }
    lines.put(
        "das.producer.initiatorserviceApiKey",
        masked("INITIATOR_API_KEY", dasProducerConfiguration.getInitiatorserviceApiKey())
    );

    kafkaConfigurationProvider.ifAvailable(kafka -> {
      lines.put("das.producer.kafka.topic", nullToEmpty(kafka.getTopic()));
      lines.put("das.producer.kafka.partitions", kafka.getPartitions());
    });

    String variant = nullToEmpty(dasProducerConfiguration.getVariant());
    if ("SimulatorBoxUnit".equalsIgnoreCase(variant)) {
      simulatorBoxUnitConfigurationProvider.ifAvailable(cfg -> putSimulatorBoxUnit(lines, cfg));
    } else if ("StaticDataUnit".equalsIgnoreCase(variant)) {
      staticDataUnitConfigurationProvider.ifAvailable(cfg -> putStaticDataUnit(lines, cfg));
    } else {
      simulatorBoxUnitConfigurationProvider.ifAvailable(cfg -> putSimulatorBoxUnit(lines, cfg));
      staticDataUnitConfigurationProvider.ifAvailable(cfg -> putStaticDataUnit(lines, cfg));
    }

    StringBuilder sb = new StringBuilder(512);
    sb.append("DAS simulator configuration:\n");
    for (Map.Entry<String, Object> e : lines.entrySet()) {
      String key = e.getKey();
      Object value = e.getValue();
      sb.append("  ")
          .append(key)
          .append(": ")
          .append(renderValue(key, value))
          .append("\n");
    }
    appendSelectedEnvironmentVariables(sb);
    return sb.toString().trim();
  }

  private void putSimulatorBoxUnit(Map<String, Object> lines, SimulatorBoxUnitConfiguration cfg) {
    lines.put(
        "das.producer.box.simulator.boxUUID",
        nullToEmpty(cfg.getBoxUUID())
    );
    lines.put(
        "das.producer.box.simulator.opticalPathUUID",
        nullToEmpty(cfg.getOpticalPathUUID())
    );
    lines.put("das.producer.box.simulator.gaugeLength", cfg.getGaugeLength());
    lines.put(
        "das.producer.box.simulator.spatialSamplingInterval",
        cfg.getSpatialSamplingInterval()
    );
    lines.put("das.producer.box.simulator.pulseWidth", cfg.getPulseWidth());
    lines.put("das.producer.box.simulator.startLocusIndex", cfg.getStartLocusIndex());
    lines.put("das.producer.box.simulator.pulseRate", cfg.getPulseRate());
    lines.put("das.producer.box.simulator.maxFreq", cfg.getMaxFreq());
    lines.put("das.producer.box.simulator.minFreq", cfg.getMinFreq());
    lines.put("das.producer.box.simulator.numberOfLoci", cfg.getNumberOfLoci());
    lines.put("das.producer.box.simulator.disableThrottling", cfg.isDisableThrottling());
    lines.put("das.producer.box.simulator.amplitudesPrPackage", cfg.getAmplitudesPrPackage());
    lines.put(
        "das.producer.box.simulator.numberOfPrePopulatedValues",
        cfg.getNumberOfPrePopulatedValues()
    );
    lines.put("das.producer.box.simulator.numberOfShots", cfg.getNumberOfShots());
    lines.put("das.producer.box.simulator.secondsToRun", cfg.getSecondsToRun());
    lines.put(
        "das.producer.box.simulator.startTimeEpochSecond",
        cfg.getStartTimeEpochSecond()
    );
    lines.put(
        "das.producer.box.simulator.timePacingEnabled",
        cfg.isTimePacingEnabled()
    );
    lines.put(
        "das.producer.box.simulator.timeLagWarnMillis",
        cfg.getTimeLagWarnMillis()
    );
    lines.put(
        "das.producer.box.simulator.timeLagDropMillis",
        cfg.getTimeLagDropMillis()
    );
    lines.put(
        "das.producer.box.simulator.amplitudeDataType",
        nullToEmpty(cfg.getAmplitudeDataType())
    );
  }

  private void putStaticDataUnit(Map<String, Object> lines, StaticDataUnitConfiguration cfg) {
    lines.put("das.producer.box.static.boxUUID", nullToEmpty(cfg.getBoxUUID()));
    lines.put("das.producer.box.static.opticalPathUUID", nullToEmpty(cfg.getOpticalPathUUID()));
    lines.put("das.producer.box.static.numberOfLoci", cfg.getNumberOfLoci());
    lines.put("das.producer.box.static.amplitudesPrPackage", cfg.getAmplitudesPrPackage());
    lines.put("das.producer.box.static.disableThrottling", cfg.isDisableThrottling());
    lines.put("das.producer.box.static.numberOfShots", cfg.getNumberOfShots());
    lines.put("das.producer.box.static.maxFreq", cfg.getMaxFreq());
    lines.put("das.producer.box.static.secondsToRun", cfg.getSecondsToRun());
    lines.put(
        "das.producer.box.static.startTimeEpochSecond",
        cfg.getStartTimeEpochSecond()
    );
    lines.put(
        "das.producer.box.static.timePacingEnabled",
        cfg.isTimePacingEnabled()
    );
    lines.put(
        "das.producer.box.static.timeLagWarnMillis",
        cfg.getTimeLagWarnMillis()
    );
    lines.put(
        "das.producer.box.static.timeLagDropMillis",
        cfg.getTimeLagDropMillis()
    );
    lines.put(
        "das.producer.box.static.amplitudeDataType",
        nullToEmpty(cfg.getAmplitudeDataType())
    );
  }

  private void appendSelectedEnvironmentVariables(StringBuilder sb) {
    String[] envVars = {
      "VENDOR_CODE",
      "PACKAGE_SIZE",
      "INITIATOR_URL",
      "ACQUISITION_START_VERSION",
      "KAFKA_SERVER_OVERRIDE",
      "SCHEMA_REGISTRY_URL_OVERRIDE",
      "VARIANT_PLUGIN",
      "REMOTE_CONTROL_ENABLED",
      "REMOTE_CONTROL_PROFILES_DIR",
      "BOX_UUID",
      "OPTICAL_PATH_UUID",
      "NUMBER_OF_LOCI",
      "NUMBER_OF_SHOTS",
      "SECONDS_TO_RUN",
      "START_TIME_EPOCH_SECOND",
      "TIME_PACING_ENABLED",
      "TIME_LAG_WARN_MS",
      "TIME_LAG_DROP_MS",
      "DISABLE_THROTTLING",
      "PULSE_RATE",
      "MAX_NYQ_FREQ",
      "MIN_NYQ_FREQ",
      "GAUGE_LENGTH",
      "SPATIAL_SAMPLING_INTERVAL",
      "PULSE_WIDTH",
      "START_LOCUS_INDEX",
      "AMPLITUDE_DATA_TYPE",
      "INITIATOR_API_KEY",
      "REMOTE_CONTROL_API_KEY"
    };

    Map<String, String> env = System.getenv();
    StringJoiner joiner = new StringJoiner("\n", "Environment variables (set):\n", "\n");
    int count = 0;
    for (String name : envVars) {
      String value = env.get(name);
      if (value == null || value.isBlank()) {
        continue;
      }
      joiner.add("  " + name + "=" + masked(name, value));
      count++;
    }
    if (count > 0) {
      sb.append("\n").append(joiner.toString().trim());
    }
  }

  private String masked(String name, String value) {
    if (value == null) {
      return "";
    }
    if (isSensitiveKey(name)) {
      return maskValue(value);
    }
    return value;
  }

  private static boolean isSensitiveKey(String key) {
    if (key == null) {
      return false;
    }
    String k = key.toLowerCase(Locale.ROOT);
    return k.contains("apikey") || k.contains("api_key") || k.contains("key")
      || k.contains("secret") || k.contains("token") || k.contains("password");
  }

  private static String maskValue(String value) {
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return "";
    }
    if (trimmed.length() <= 4) {
      return "****";
    }
    return trimmed.substring(0, 2) + "****" + trimmed.substring(trimmed.length() - 2);
  }

  private static String renderValue(String key, Object value) {
    if (value == null) {
      return "";
    }
    if (value instanceof String s) {
      if (isSensitiveKey(key)) {
        return maskValue(s);
      }
      return s;
    }
    return Objects.toString(value);
  }

  private static String join(String[] values) {
    if (values == null || values.length == 0) {
      return "";
    }
    return String.join(",", values);
  }

  private static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }
}
