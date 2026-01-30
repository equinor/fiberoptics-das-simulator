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

import com.equinor.fiberoptics.das.DasProducerFactory;
import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.GenericDasProducer;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnitConfiguration;
import com.equinor.fiberoptics.das.remotecontrol.RemoteControlService;
import com.equinor.fiberoptics.das.remotecontrol.profile.DasSimulatorProfileResolver;
import com.equinor.kafka.KafkaRelay;
import com.equinor.kafka.KafkaSender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanFactory;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Integration-style unit test for {@link RemoteControlService} using the real profile
 * JSON stored in {@code remote-control-profiles}.
 *
 * <p>This test verifies the core principle of remote-control mode: the simulator should
 * ignore the incoming APPLY request body (except Custom.das-simulator-profile) and
 * instead load and apply the configuration from the on-disk profile.</p>
 *
 * <p>We assert that simulator configuration values are copied from the resolved profile,
 * not from the request, and that the producer factory is invoked with the profile JSON.</p>
 */
public class RemoteControlExampleAcquisitionAppliedTest {

  @Test
  public void apply_usesExampleAcquisitionValuesAsSimulatorConfig() throws Exception {
    // Arrange: load a real profile file from the repo to keep this test aligned with shipped examples.
    String profileId = "019c0d85-dc19-7dfe-859e-8d0c066f3c46";
    String profileJson = Files.readString(Path.of("..", "remote-control-profiles", profileId + ".json"));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(profileJson);
    // The request body contains conflicting values; they must be ignored in favor of the profile.
    String requestJson = "{\"Custom\":{\"das-simulator-profile\":\"" + profileId + "\"},\"NumberOfLoci\":9999,\"MeasurementStartTime\":\"2099-01-01T00:00:00Z\"}";

    // Seed the simulator config with obviously wrong values so we can see that APPLY replaces them.
    SimulatorBoxUnitConfiguration simCfg = new SimulatorBoxUnitConfiguration();
    simCfg.setNumberOfLoci(1);
    simCfg.setStartLocusIndex(123);
    simCfg.setPulseRate(1);
    simCfg.setMaxFreq(1);
    simCfg.setSpatialSamplingInterval(1);
    simCfg.setGaugeLength(1);
    simCfg.setPulseWidth(1);
    simCfg.setBoxUUID("wrong");
    simCfg.setOpticalPathUUID("wrong");

    DasProducerConfiguration dasCfg = new DasProducerConfiguration();
    dasCfg.setVariant("SimulatorBoxUnit");
    dasCfg.getRemoteControl().setProfilesDirectory(Path.of("..", "remote-control-profiles").toString());

    // Wire minimal collaborators with mocks so we can observe calls without Kafka side effects.
    BeanFactory beanFactory = mock(BeanFactory.class);
    KafkaRelay kafkaRelay = mock(KafkaRelay.class);
    KafkaSender kafkaSender = mock(KafkaSender.class);
    DasProducerFactory dasProducerFactory = mock(DasProducerFactory.class);
    DasSimulatorProfileResolver profileResolver = new DasSimulatorProfileResolver(dasCfg, objectMapper);
    @SuppressWarnings("unchecked")
    KafkaProducer<DASMeasurementKey, DASMeasurement> kafkaProducer = mock(KafkaProducer.class);
    when(dasProducerFactory.createProducerFromAcquisitionJson(anyString())).thenReturn(kafkaProducer);

    // Producer asserts that the in-memory config values were updated from the profile JSON.
    GenericDasProducer producer = () -> {
      assertEquals(root.get("NumberOfLoci").asInt(), simCfg.getNumberOfLoci());
      assertEquals(root.get("StartLocusIndex").asInt(), simCfg.getStartLocusIndex());
      assertEquals((int) Math.round(root.get("PulseRate").asDouble()), simCfg.getPulseRate());
      assertEquals((float) root.get("MaximumFrequency").asDouble(), simCfg.getMaxFreq());
      assertEquals((float) root.get("SpatialSamplingInterval").asDouble(), simCfg.getSpatialSamplingInterval());
      assertEquals((float) root.get("GaugeLength").asDouble(), simCfg.getGaugeLength());
      assertEquals((float) root.get("PulseWidth").asDouble(), simCfg.getPulseWidth());
      assertEquals(root.get("DasInstrumentBoxUUID").asText(), simCfg.getBoxUUID());
      assertEquals(root.get("OpticalPathUUID").asText(), simCfg.getOpticalPathUUID());
      assertEquals(root.get("VendorCode").asText(), dasCfg.getVendorCode());
      return Flux.<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>>empty();
    };
    when(beanFactory.getBean(eq("SimulatorBoxUnit"), eq(GenericDasProducer.class))).thenReturn(producer);

    RemoteControlService service = new RemoteControlService(
      beanFactory,
      dasCfg,
      simCfg,
      dasProducerFactory,
      kafkaRelay,
      kafkaSender,
      objectMapper,
      profileResolver);

    // Act: apply the remote-control request.
    service.apply(requestJson);

    // Assert: the producer factory and sender are wired using the resolved profile JSON.
    ArgumentCaptor<String> acquisitionCaptor = ArgumentCaptor.forClass(String.class);
    verify(dasProducerFactory, times(1)).createProducerFromAcquisitionJson(acquisitionCaptor.capture());
    verify(kafkaSender).setProducer(eq(kafkaProducer));

    // Sanity checks that critical identifiers are populated from the profile.
    assertNotNull(simCfg.getBoxUUID());
    assertNotNull(simCfg.getOpticalPathUUID());

    // The acquisition JSON sent to the initiator should reuse profile values but with a new id/time.
    JsonNode captured = objectMapper.readTree(acquisitionCaptor.getValue());
    assertEquals(root.get("NumberOfLoci").asInt(), captured.get("NumberOfLoci").asInt());
    assertEquals(root.get("OpticalPathUUID").asText(), captured.get("OpticalPathUUID").asText());
    assertEquals(root.get("DasInstrumentBoxUUID").asText(), captured.get("DasInstrumentBoxUUID").asText());
    assertEquals(root.get("VendorCode").asText(), captured.get("VendorCode").asText());
    assertNotNull(captured.get("AcquisitionId"));
    assertNotNull(captured.get("MeasurementStartTime"));
    assertNotEquals(root.get("AcquisitionId").asText(), captured.get("AcquisitionId").asText());
    assertNotEquals(root.get("MeasurementStartTime").asText(), captured.get("MeasurementStartTime").asText());
  }
}
