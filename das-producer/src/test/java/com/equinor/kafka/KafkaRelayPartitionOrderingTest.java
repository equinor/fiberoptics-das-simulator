/*-
 * ========================LICENSE_START=================================
 * fiberoptics-das-simulator
 * %%
 * Copyright (C) 2020 - 2021 Equinor ASA
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
package com.equinor.kafka;

import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Verifies two invariants that are critical for consumers of the simulated DAS stream:
 *
 * <ol>
 *   <li><b>Deterministic routing:</b> a given {@code locus} must always be sent to the same Kafka
 *   {@code partition} (based on {@link DasProducerConfiguration#getPartitionAssignments()}). This is what makes
 *   it safe to scale consumers and still reason about where each locus ends up.</li>
 *   <li><b>Per-partition ordering:</b> within a partition, records must be emitted in the same order as they are
 *   handed to {@link KafkaRelay}. Kafka only guarantees ordering <i>within</i> a partition, so we must not break
 *   that guarantee on the producer side.</li>
 * </ol>
 *
 * <p>The test is parameterized to run with both a single Kafka producer instance and multiple producer instances.
 * When multiple producer instances are configured, {@link KafkaSender} deterministically chooses which producer
 * to use based on {@code partition % producerCount}. The test asserts that this sharding does not change routing
 * or ordering.</p>
 *
 * <p>Implementation-wise, this is exercised by mocking {@link KafkaProducer#send(ProducerRecord, org.apache.kafka.clients.producer.Callback)}
 * and capturing:</p>
 * <ul>
 *   <li>the observed {@code partition} for each {@code locus}</li>
 *   <li>the sequence of record timestamps per partition (used as an easy monotonic sequence marker)</li>
 *   <li>which producer instance index handled each partition when multiple producers are configured</li>
 * </ul>
 */
class KafkaRelayPartitionOrderingTest {

  @ParameterizedTest
  @ValueSource(ints = {1, 3})
  void locusAlwaysMapsToSamePartition_andOrderingWithinPartitionIsPreserved_evenWithMultipleKafkaProducers(int producerInstances)
    throws Exception {
    KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setTopic("topic");
    kafkaConfiguration.setPartitions(4);
    kafkaConfiguration.setRelayQueueCapacity(1000);
    kafkaConfiguration.setRelayEnqueueTimeoutMillis(5_000);

    DasProducerConfiguration producerConfiguration = new DasProducerConfiguration();
    Map<Integer, Integer> locusToPartition = Map.of(
      0, 0,
      1, 1,
      2, 2,
      3, 3
    );
    producerConfiguration.setPartitionAssignments(locusToPartition);

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    KafkaSender kafkaSender = new KafkaSender(meterRegistry);

    Map<Integer, Set<Integer>> partitionsObservedByLocus = new ConcurrentHashMap<>();
    Map<Integer, ConcurrentLinkedQueue<Long>> timestampsByPartition = new ConcurrentHashMap<>();
    Map<Integer, ConcurrentLinkedQueue<Integer>> producerIndexByPartition = new ConcurrentHashMap<>();

    CountDownLatch sent = new CountDownLatch(40);
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers = new ArrayList<>();
    for (int producerIndex = 0; producerIndex < producerInstances; producerIndex++) {
      @SuppressWarnings("unchecked")
      KafkaProducer<DASMeasurementKey, DASMeasurement> producer = mock(KafkaProducer.class);
      int idx = producerIndex;
      doAnswer(invocation -> {
        @SuppressWarnings("unchecked")
        ProducerRecord<DASMeasurementKey, DASMeasurement> record = invocation.getArgument(0, ProducerRecord.class);
        assertNotNull(record.partition(), "Expected partition to be set on ProducerRecord");
        int partition = record.partition();
        int locus = record.value().getLocus();
        long timestamp = record.timestamp() == null ? -1L : record.timestamp();
        partitionsObservedByLocus.computeIfAbsent(locus, k -> ConcurrentHashMap.newKeySet()).add(partition);
        timestampsByPartition.computeIfAbsent(partition, p -> new ConcurrentLinkedQueue<>()).add(timestamp);
        producerIndexByPartition.computeIfAbsent(partition, p -> new ConcurrentLinkedQueue<>()).add(idx);
        sent.countDown();
        return CompletableFuture.completedFuture(null);
      }).when(producer).send(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.isNull());
      producers.add(producer);
    }
    kafkaSender.setProducers(producers);

    KafkaRelay relay = new KafkaRelay(kafkaConfiguration, kafkaSender, producerConfiguration);

    Map<Integer, List<Long>> expectedTimestampsByPartition = new ConcurrentHashMap<>();
    for (int i = 0; i < 40; i++) {
      int locus = i % 4;
      long startSnapshotTimeNano = i * 1_000_000L;
      PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry = entry(locus, startSnapshotTimeNano);
      relay.relayToKafka(entry);

      int expectedPartition = locusToPartition.get(locus);
      long expectedTimestamp = startSnapshotTimeNano / 1_000_000L;
      expectedTimestampsByPartition.computeIfAbsent(expectedPartition, p -> new ArrayList<>()).add(expectedTimestamp);
    }

    assertTrue(sent.await(5, TimeUnit.SECONDS), "Timed out waiting for records to be sent");
    relay.teardown();

    for (Map.Entry<Integer, Integer> mapping : locusToPartition.entrySet()) {
      int locus = mapping.getKey();
      int expectedPartition = mapping.getValue();
      Set<Integer> observed = partitionsObservedByLocus.get(locus);
      assertNotNull(observed, "Expected locus " + locus + " to have been observed");
      assertEquals(Set.of(expectedPartition), observed, "Expected locus to always map to the same partition");
    }

    for (Map.Entry<Integer, List<Long>> expected : expectedTimestampsByPartition.entrySet()) {
      int partition = expected.getKey();
      List<Long> expectedTimestamps = expected.getValue();
      List<Long> actualTimestamps = new ArrayList<>(timestampsByPartition.getOrDefault(partition, new ConcurrentLinkedQueue<>()));
      assertEquals(expectedTimestamps, actualTimestamps, "Expected per-partition send order to be preserved");

      List<Integer> producerIndexes = new ArrayList<>(producerIndexByPartition.getOrDefault(partition, new ConcurrentLinkedQueue<>()));
      int expectedProducerIndex = Math.floorMod(partition, producerInstances);
      for (Integer idx : producerIndexes) {
        assertEquals(expectedProducerIndex, idx, "Expected partition to always be routed to the same producer instance");
      }
    }
  }

  private static PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry(int locus, long startSnapshotTimeNano) {
    DASMeasurementKey key = DASMeasurementKey.newBuilder().setLocus(locus).build();
    DASMeasurement value = DASMeasurement.newBuilder()
      .setLocus(locus)
      .setStartSnapshotTimeNano(startSnapshotTimeNano)
      .setTrustedTimeSource(true)
      .setAmplitudesFloat(List.of())
      .setAmplitudesLong(List.of())
      .build();
    return new PartitionKeyValueEntry<>(key, value, locus);
  }
}
