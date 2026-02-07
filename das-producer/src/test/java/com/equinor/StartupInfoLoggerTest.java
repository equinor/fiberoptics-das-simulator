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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.info.BuildProperties;
import org.springframework.core.env.Environment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StartupInfoLoggerTest {

  @Test
  public void buildStartupMessage_masksSecretsAndIncludesVariantFields() throws Exception {
    Environment environment = mock(Environment.class);
    when(environment.getActiveProfiles()).thenReturn(new String[] {"dev", "test"});

    DasProducerConfiguration producerConfiguration = new DasProducerConfiguration();
    producerConfiguration.setVariant("SimulatorBoxUnit");
    producerConfiguration.setVendorCode("Simulator");
    producerConfiguration.setAmplitudesPrPackage(64);
    producerConfiguration.setInitiatorserviceUrl("http://localhost:8080");
    producerConfiguration.setAcquisitionStartVersion("V2");
    producerConfiguration.setOverrideBootstrapServersWith("localhost:9092");
    producerConfiguration.setOverrideSchemaRegistryWith("http://localhost:8081");
    producerConfiguration.setInitiatorserviceApiKey("abcd1234");
    producerConfiguration.getRemoteControl().setEnabled(true);
    producerConfiguration.getRemoteControl().setProfilesDirectory("profiles");
    producerConfiguration.getRemoteControl().setApiKey("rc-1234");

    SimulatorBoxUnitConfiguration simulatorConfig = new SimulatorBoxUnitConfiguration();
    simulatorConfig.setBoxUUID("box-id");
    simulatorConfig.setOpticalPathUUID("optical-id");
    simulatorConfig.setGaugeLength(10.5f);
    simulatorConfig.setSpatialSamplingInterval(1.2f);
    simulatorConfig.setPulseWidth(100.5f);
    simulatorConfig.setStartLocusIndex(0);
    simulatorConfig.setPulseRate(10000);
    simulatorConfig.setMaxFreq(5000f);
    simulatorConfig.setMinFreq(0f);
    simulatorConfig.setNumberOfLoci(128);
    simulatorConfig.setDisableThrottling(false);
    simulatorConfig.setAmplitudesPrPackage(64);
    simulatorConfig.setNumberOfPrePopulatedValues(10);
    simulatorConfig.setNumberOfShots(5);
    simulatorConfig.setSecondsToRun(30);
    simulatorConfig.setStartTimeEpochSecond(0L);
    simulatorConfig.setTimePacingEnabled(true);
    simulatorConfig.setTimeLagWarnMillis(500L);
    simulatorConfig.setTimeLagDropMillis(2000L);
    simulatorConfig.setAmplitudeDataType("FLOAT");

    KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setTopic("topic");
    kafkaConfiguration.setPartitions(3);

    StartupInfoLogger logger = new StartupInfoLogger(
        environment,
        new TestObjectProvider<>(null),
        producerConfiguration,
        new TestObjectProvider<>(simulatorConfig),
        new TestObjectProvider<>(new StaticDataUnitConfiguration()),
        new TestObjectProvider<>(kafkaConfiguration)
    );

    Method method = StartupInfoLogger.class.getDeclaredMethod("buildStartupMessage");
    method.setAccessible(true);
    String message = (String) method.invoke(logger);

    assertTrue(message.contains("DAS simulator configuration:"));
    assertTrue(message.contains("profiles.active: dev,test"));
    assertTrue(message.contains("das.producer.variant: SimulatorBoxUnit"));
    assertTrue(message.contains("das.producer.box.simulator.boxUUID: box-id"));
    assertTrue(message.contains("das.producer.kafka.topic: topic"));
    assertTrue(message.contains("das.producer.kafka.partitions: 3"));
    assertTrue(message.contains("das.producer.initiatorserviceApiKey: ab****34"));
    assertTrue(message.contains("das.producer.remoteControl.apiKey: rc****34"));
    assertFalse(message.contains("abcd1234"));
    assertFalse(message.contains("rc-1234"));
  }

  private static class TestObjectProvider<T> implements ObjectProvider<T> {
    private final T value;

    private TestObjectProvider(T value) {
      this.value = value;
    }

    @Override
    public T getObject(Object... args) {
      return value;
    }

    @Override
    public T getObject() {
      return value;
    }

    @Override
    public T getIfAvailable() {
      return value;
    }

    @Override
    public T getIfUnique() {
      return value;
    }

    @Override
    public void ifAvailable(Consumer<T> dependencyConsumer) {
      if (value != null) {
        dependencyConsumer.accept(value);
      }
    }

    @Override
    public void ifUnique(Consumer<T> dependencyConsumer) {
      if (value != null) {
        dependencyConsumer.accept(value);
      }
    }

    @Override
    public Iterator<T> iterator() {
      return value == null ? Arrays.<T>asList().iterator() : Arrays.asList(value).iterator();
    }

    @Override
    public Stream<T> stream() {
      return value == null ? Stream.empty() : Stream.of(value);
    }
  }
}
