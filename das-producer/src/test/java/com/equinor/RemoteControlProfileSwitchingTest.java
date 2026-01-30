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
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnitConfiguration;
import com.equinor.fiberoptics.das.remotecontrol.RemoteControlService;
import com.equinor.fiberoptics.das.remotecontrol.profile.DasSimulatorProfileResolver;
import com.equinor.kafka.KafkaRelay;
import com.equinor.kafka.KafkaSender;
import com.fasterxml.jackson.databind.ObjectMapper;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.BeanFactory;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests profile switching semantics for remote-control APPLY requests.
 *
 * <p>The simulator should:</p>
 * <ul>
 *   <li>Use profile files (not request body fields) to configure the producer.</li>
 *   <li>Treat APPLY with the same profile as idempotent, even if AcquisitionId/MeasurementStartTime differ.</li>
 *   <li>Send a best-effort STOP to the stream initiator when switching to a new profile while running.</li>
 *   <li>Continue switching even if the STOP call fails.</li>
 * </ul>
 */
public class RemoteControlProfileSwitchingTest {

  @Test
  public void apply_switchesProfiles_sendsBestEffortStop_andIgnoresRequestBodyFields(@TempDir Path tempDir) throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();

    // Arrange: create two distinct profile files and place them in a temp folder.
    String profileId1 = "11111111-1111-1111-1111-111111111111";
    String profileId2 = "22222222-2222-2222-2222-222222222222";

    String acquisitionId1 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    String acquisitionId2 = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    String profileJson1 = """
      {
        "NumberOfLoci": 25,
        "SpatialSamplingInterval": 1.1,
        "PulseWidthUnit": "ns",
        "Custom": { "das-simulator-profile": "%s" },
        "OpticalPathUUID": "9f79c244-1fec-4c78-83f9-e4b001f1c40f",
        "DasInstrumentBoxUUID": "00528e45-06d0-4110-bba4-e904aaa02657",
        "GaugeLength": 10.209524,
        "MaximumFrequency": 5000,
        "StartLocusIndex": 0,
        "PulseWidth": 100.50,
        "AcquisitionId": "%s",
        "PulseRate": 10000,
        "VendorCode": "Simulator",
        "MeasurementStartTime": "2026-01-27T08:56:35Z"
      }
      """.formatted(profileId1, acquisitionId1).trim();

    String profileJson2 = """
      {
        "NumberOfLoci": 25,
        "SpatialSamplingInterval": 1.1,
        "PulseWidthUnit": "ns",
        "Custom": { "das-simulator-profile": "%s" },
        "OpticalPathUUID": "00528e45-06d0-4110-bba4-e904afe02657",
        "DasInstrumentBoxUUID": "00528e45-06d0-4110-bba4-e904aaa02657",
        "GaugeLength": 10.209524,
        "MaximumFrequency": 5000,
        "StartLocusIndex": 0,
        "PulseWidth": 101.50,
        "AcquisitionId": "%s",
        "PulseRate": 10000,
        "VendorCode": "Simulator",
        "MeasurementStartTime": "2026-01-27T08:56:35Z"
      }
      """.formatted(profileId2, acquisitionId2).trim();

    Files.writeString(tempDir.resolve(profileId1 + ".json"), profileJson1, StandardCharsets.UTF_8);
    Files.writeString(tempDir.resolve(profileId2 + ".json"), profileJson2, StandardCharsets.UTF_8);

    DasProducerConfiguration dasCfg = new DasProducerConfiguration();
    dasCfg.setVariant("SimulatorBoxUnit");
    dasCfg.getRemoteControl().setProfilesDirectory(tempDir.toString());

    SimulatorBoxUnitConfiguration simCfg = new SimulatorBoxUnitConfiguration();

    // Mock collaborators to keep the test focused on profile switching logic.
    BeanFactory beanFactory = mock(BeanFactory.class);
    KafkaRelay kafkaRelay = mock(KafkaRelay.class);
    KafkaSender kafkaSender = mock(KafkaSender.class);
    DasProducerFactory dasProducerFactory = mock(DasProducerFactory.class);
    DasSimulatorProfileResolver profileResolver = new DasSimulatorProfileResolver(dasCfg, objectMapper);

    @SuppressWarnings("unchecked")
    KafkaProducer<DASMeasurementKey, DASMeasurement> kafkaProducer = mock(KafkaProducer.class);
    AtomicReference<String> firstAcquisitionId = new AtomicReference<>();
    AtomicReference<String> secondAcquisitionId = new AtomicReference<>();
    when(dasProducerFactory.createProducerFromAcquisitionJson(anyString())).thenAnswer(invocation -> {
      String json = invocation.getArgument(0, String.class);
      String id = objectMapper.readTree(json).get("AcquisitionId").asText();
      if (firstAcquisitionId.get() == null) {
        firstAcquisitionId.set(id);
      } else {
        secondAcquisitionId.set(id);
      }
      return kafkaProducer;
    });

    // Simulate a running producer (never completes) so APPLY must treat it as "active".
    GenericDasProducer neverEndingProducer = () -> Flux.never();
    when(beanFactory.getBean(eq("SimulatorBoxUnit"), eq(GenericDasProducer.class))).thenReturn(neverEndingProducer);

    RemoteControlService service = new RemoteControlService(
      beanFactory,
      dasCfg,
      simCfg,
      dasProducerFactory,
      kafkaRelay,
      kafkaSender,
      objectMapper,
      profileResolver);

    // Request 1: uses profileId1 but includes conflicting fields that must be ignored.
    String request1 = "{\"Custom\":{\"das-simulator-profile\":\"" + profileId1 + "\"},\"NumberOfLoci\":9999,\"AcquisitionId\":\"ignored\"}";
    // Request 1 (variant): same profile, different AcquisitionId and MeasurementStartTime -> should be idempotent.
    String request1DifferentId = "{\"Custom\":{\"das-simulator-profile\":\"" + profileId1 + "\"},\"AcquisitionId\":\"ignored2\",\"MeasurementStartTime\":\"2099-01-01T00:00:00Z\"}";
    // Request 2: switches to profileId2 and should trigger a best-effort stop of the previous acquisition.
    String request2 = "{\"Custom\":{\"das-simulator-profile\":\"" + profileId2 + "\"}}";

    // Act: first apply starts the producer based on profileId1.
    service.apply(request1);
    verify(dasProducerFactory, times(1)).createProducerFromAcquisitionJson(anyString());
    assertNotNull(firstAcquisitionId.get());

    // Act: apply the same profile again (with different ids) should not recreate producer.
    service.apply(request1DifferentId);
    verify(dasProducerFactory, times(1)).createProducerFromAcquisitionJson(anyString());

    // Act: switching profiles should attempt a stop; even if it fails, switching must continue.
    doThrow(new RuntimeException("stop endpoint missing")).when(dasProducerFactory).stopAcquisitionBestEffort(anyString());
    service.apply(request2);
    verify(dasProducerFactory, times(2)).createProducerFromAcquisitionJson(anyString());
    verify(dasProducerFactory).stopAcquisitionBestEffort(eq(firstAcquisitionId.get()));
    if (secondAcquisitionId.get() != null) {
      assertNotEquals(firstAcquisitionId.get(), secondAcquisitionId.get());
    }
  }

  @Test
  public void stop_sendsBestEffortStop_withGeneratedAcquisitionId(@TempDir Path tempDir) throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    String profileId = "33333333-3333-3333-3333-333333333333";
    String storedAcquisitionId = "cccccccc-cccc-cccc-cccc-cccccccccccc";
    String profileJson = """
      {
        "NumberOfLoci": 25,
        "SpatialSamplingInterval": 1.1,
        "PulseWidthUnit": "ns",
        "Custom": { "das-simulator-profile": "%s" },
        "OpticalPathUUID": "9f79c244-1fec-4c78-83f9-e4b001f1c40f",
        "DasInstrumentBoxUUID": "00528e45-06d0-4110-bba4-e904aaa02657",
        "GaugeLength": 10.209524,
        "MaximumFrequency": 5000,
        "StartLocusIndex": 0,
        "PulseWidth": 100.50,
        "AcquisitionId": "%s",
        "PulseRate": 10000,
        "VendorCode": "Simulator",
        "MeasurementStartTime": "2026-01-27T08:56:35Z"
      }
      """.formatted(profileId, storedAcquisitionId).trim();

    Files.writeString(tempDir.resolve(profileId + ".json"), profileJson, StandardCharsets.UTF_8);

    DasProducerConfiguration dasCfg = new DasProducerConfiguration();
    dasCfg.setVariant("SimulatorBoxUnit");
    dasCfg.getRemoteControl().setProfilesDirectory(tempDir.toString());

    SimulatorBoxUnitConfiguration simCfg = new SimulatorBoxUnitConfiguration();

    BeanFactory beanFactory = mock(BeanFactory.class);
    KafkaRelay kafkaRelay = mock(KafkaRelay.class);
    KafkaSender kafkaSender = mock(KafkaSender.class);
    DasProducerFactory dasProducerFactory = mock(DasProducerFactory.class);
    DasSimulatorProfileResolver profileResolver = new DasSimulatorProfileResolver(dasCfg, objectMapper);

    @SuppressWarnings("unchecked")
    KafkaProducer<DASMeasurementKey, DASMeasurement> kafkaProducer = mock(KafkaProducer.class);
    AtomicReference<String> generatedAcquisitionId = new AtomicReference<>();
    when(dasProducerFactory.createProducerFromAcquisitionJson(anyString())).thenAnswer(invocation -> {
      String json = invocation.getArgument(0, String.class);
      generatedAcquisitionId.set(objectMapper.readTree(json).get("AcquisitionId").asText());
      return kafkaProducer;
    });

    GenericDasProducer neverEndingProducer = () -> Flux.never();
    when(beanFactory.getBean(eq("SimulatorBoxUnit"), eq(GenericDasProducer.class))).thenReturn(neverEndingProducer);

    RemoteControlService service = new RemoteControlService(
      beanFactory,
      dasCfg,
      simCfg,
      dasProducerFactory,
      kafkaRelay,
      kafkaSender,
      objectMapper,
      profileResolver);

    service.apply("{\"Custom\":{\"das-simulator-profile\":\"" + profileId + "\"}}");
    assertNotNull(generatedAcquisitionId.get());
    assertNotEquals(storedAcquisitionId, generatedAcquisitionId.get());

    service.stop(Optional.empty());
    verify(dasProducerFactory).stopAcquisitionBestEffort(eq(generatedAcquisitionId.get()));
  }
}
