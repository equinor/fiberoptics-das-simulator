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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class KafkaRelayBackpressureTest {

  @Test
  void blocksWhenPerPartitionQueueIsFull() throws Exception {
    KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setTopic("topic");
    kafkaConfiguration.setPartitions(1);
    kafkaConfiguration.setRelayQueueCapacity(1);
    kafkaConfiguration.setRelayEnqueueTimeoutMillis(0);

    DasProducerConfiguration producerConfiguration = new DasProducerConfiguration();
    producerConfiguration.setPartitionAssignments(Map.of(0, 0));

    KafkaSender kafkaSender = mock(KafkaSender.class);
    CountDownLatch firstSendStarted = new CountDownLatch(1);
    CountDownLatch allowSendToProceed = new CountDownLatch(1);
    doAnswer(invocation -> {
      firstSendStarted.countDown();
      allowSendToProceed.await(5, TimeUnit.SECONDS);
      return null;
    }).when(kafkaSender).send(any());

    KafkaRelay relay = new KafkaRelay(kafkaConfiguration, kafkaSender, producerConfiguration);
    PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry = entry(0);

    relay.relayToKafka(entry);
    assertTrue(firstSendStarted.await(2, TimeUnit.SECONDS), "Expected first send to start");

    relay.relayToKafka(entry); // fills queue (capacity=1)

    CompletableFuture<Void> blocked = CompletableFuture.runAsync(() -> relay.relayToKafka(entry));
    Thread.sleep(200);
    assertFalse(blocked.isDone(), "Expected relayToKafka() to block when queue is full");

    allowSendToProceed.countDown();
    blocked.get(2, TimeUnit.SECONDS);

    relay.teardown();
  }

  @Test
  void timesOutInsteadOfBlockingForeverWhenEnqueueTimeoutIsConfigured() throws Exception {
    KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setTopic("topic");
    kafkaConfiguration.setPartitions(1);
    kafkaConfiguration.setRelayQueueCapacity(1);
    kafkaConfiguration.setRelayEnqueueTimeoutMillis(100);

    DasProducerConfiguration producerConfiguration = new DasProducerConfiguration();
    producerConfiguration.setPartitionAssignments(Map.of(0, 0));

    KafkaSender kafkaSender = mock(KafkaSender.class);
    CountDownLatch firstSendStarted = new CountDownLatch(1);
    CountDownLatch allowSendToProceed = new CountDownLatch(1);
    doAnswer(invocation -> {
      firstSendStarted.countDown();
      allowSendToProceed.await(5, TimeUnit.SECONDS);
      return null;
    }).when(kafkaSender).send(any());

    KafkaRelay relay = new KafkaRelay(kafkaConfiguration, kafkaSender, producerConfiguration);
    PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry = entry(0);

    relay.relayToKafka(entry);
    assertTrue(firstSendStarted.await(2, TimeUnit.SECONDS), "Expected first send to start");
    relay.relayToKafka(entry); // fills queue (capacity=1)

    long startNanos = System.nanoTime();
    assertDoesNotThrow(() -> relay.relayToKafka(entry), "Expected timeout path to be handled without throwing");
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    assertTrue(elapsedMillis >= 50 && elapsedMillis < 1500, "Expected relayToKafka() to return after configured timeout");

    allowSendToProceed.countDown();
    relay.teardown();
  }

  @Test
  void backpressureIsPerPartition() throws Exception {
    KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setTopic("topic");
    kafkaConfiguration.setPartitions(2);
    kafkaConfiguration.setRelayQueueCapacity(1);
    kafkaConfiguration.setRelayEnqueueTimeoutMillis(0);

    DasProducerConfiguration producerConfiguration = new DasProducerConfiguration();
    producerConfiguration.setPartitionAssignments(Map.of(
      0, 0,
      1, 1
    ));

    KafkaSender kafkaSender = mock(KafkaSender.class);
    CountDownLatch partition0SendStarted = new CountDownLatch(1);
    CountDownLatch allowPartition0Send = new CountDownLatch(1);
    doAnswer(invocation -> {
      ProducerRecord<?, ?> record = invocation.getArgument(0);
      Integer partition = record.partition();
      if (partition != null && partition == 0) {
        partition0SendStarted.countDown();
        allowPartition0Send.await(5, TimeUnit.SECONDS);
      }
      return null;
    }).when(kafkaSender).send(any());

    KafkaRelay relay = new KafkaRelay(kafkaConfiguration, kafkaSender, producerConfiguration);

    PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entryPartition0 = entry(0);
    PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entryPartition1 = entry(1);

    relay.relayToKafka(entryPartition0);
    assertTrue(partition0SendStarted.await(2, TimeUnit.SECONDS), "Expected partition 0 send to start");
    relay.relayToKafka(entryPartition0); // fills partition 0 queue

    CompletableFuture<Void> partition1 = CompletableFuture.runAsync(() -> relay.relayToKafka(entryPartition1));
    assertDoesNotThrow(() -> partition1.get(500, TimeUnit.MILLISECONDS), "Partition 1 should not be blocked by partition 0");

    CompletableFuture<Void> blockedPartition0 = CompletableFuture.runAsync(() -> relay.relayToKafka(entryPartition0));
    Thread.sleep(200);
    assertFalse(blockedPartition0.isDone(), "Expected only partition 0 to be backpressured");

    allowPartition0Send.countDown();
    assertDoesNotThrow(() -> blockedPartition0.get(2, TimeUnit.SECONDS));

    relay.teardown();
  }

  private static PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry(int locus) {
    DASMeasurementKey key = DASMeasurementKey.newBuilder().setLocus(locus).build();
    DASMeasurement value = DASMeasurement.newBuilder()
      .setLocus(locus)
      .setStartSnapshotTimeNano(0L)
      .setTrustedTimeSource(true)
      .setAmplitudesFloat(Collections.emptyList())
      .setAmplitudesLong(Collections.emptyList())
      .build();
    return new PartitionKeyValueEntry<>(key, value, locus);
  }
}
