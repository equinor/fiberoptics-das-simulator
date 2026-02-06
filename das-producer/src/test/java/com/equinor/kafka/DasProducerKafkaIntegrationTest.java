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
package com.equinor.kafka;

import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnit;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnitConfiguration;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class DasProducerKafkaIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(DasProducerKafkaIntegrationTest.class);
  private static final String CONFLUENT_VERSION = "7.7.1";
  private static final Network NETWORK = Network.newNetwork();

  @Container
  private static final KafkaContainer KAFKA =
    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
      .withNetwork(NETWORK)
      .withNetworkAliases("kafka");

  @Container
  private static final GenericContainer<?> SCHEMA_REGISTRY =
    new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:" + CONFLUENT_VERSION))
      .withNetwork(NETWORK)
      .withNetworkAliases("schemaregistry")
      .withExposedPorts(8081)
      .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schemaregistry")
      .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
      .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
      .waitingFor(Wait.forHttp("/subjects").forPort(8081).forStatusCode(200));

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void produces50ShotsWith500Loci_at10kHz_andWritesExpectedKafkaRecordCount() throws Exception {
    String topic = "das-producer-it-" + UUID.randomUUID();
    int shots = 50;
    int loci = 500;
    long expectedRecords = (long) shots * (long) loci;

    createTopic(topic, 1);
    purgeTopic(topic);

    String schemaRegistryUrl = "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);

    logger.info(
      "DAS producer integration-test configuration: topic={}, shots={}, loci={}, pulseRateHz={}, maxFreqHz={}, amplitudesPrPackage={}, amplitudeDataType={}, disableThrottling={}, expectedRecords={}, bootstrapServers={}, schemaRegistryUrl={}",
      topic, shots, loci, 10_000, 10_000, 256, "long", true, expectedRecords, KAFKA.getBootstrapServers(), schemaRegistryUrl
    );
    logger.info("Note: this test runs the producer implementation directly (no Spring Boot app), so StartupInfoLogger is not invoked.");

    KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setTopic(topic);
    kafkaConfiguration.setPartitions(1);
    kafkaConfiguration.setProducerInstances(1);
    kafkaConfiguration.setRelayQueueCapacity(5_000);
    kafkaConfiguration.setRelayEnqueueTimeoutMillis(30_000);
    kafkaConfiguration.setRelayEnqueueWarnMillis(0);

    Map<String, Object> producerProps = kafkaConfiguration.kafkaProperties(KAFKA.getBootstrapServers(), schemaRegistryUrl);
    try (KafkaProducer<DASMeasurementKey, DASMeasurement> kafkaProducer = new KafkaProducer<>(producerProps)) {
      KafkaSender kafkaSender = new KafkaSender(new SimpleMeterRegistry());
      kafkaSender.setProducers(List.of(kafkaProducer));

      DasProducerConfiguration dasProducerConfiguration = new DasProducerConfiguration();
      dasProducerConfiguration.setPartitionAssignments(locusToSinglePartitionAssignments(loci));

      KafkaRelay kafkaRelay = new KafkaRelay(kafkaConfiguration, kafkaSender, dasProducerConfiguration);

      SimulatorBoxUnitConfiguration simulatorConfiguration = new SimulatorBoxUnitConfiguration();
      simulatorConfiguration.setAmplitudeDataType("long");
      simulatorConfiguration.setNumberOfShots(shots);
      simulatorConfiguration.setSecondsToRun(1);
      simulatorConfiguration.setPulseRate(10_000);
      simulatorConfiguration.setMaxFreq(10_000);
      simulatorConfiguration.setMinFreq(0);
      simulatorConfiguration.setBoxUUID("00000000-0000-0000-0000-000000000000");
      simulatorConfiguration.setOpticalPathUUID("00000000-0000-0000-0000-000000000000");
      simulatorConfiguration.setSpatialSamplingInterval(1.1f);
      simulatorConfiguration.setPulseWidth(100.5f);
      simulatorConfiguration.setStartLocusIndex(0);
      simulatorConfiguration.setDisableThrottling(true);
      simulatorConfiguration.setGaugeLength(10.209524f);
      simulatorConfiguration.setNumberOfLoci(loci);
      simulatorConfiguration.setNumberOfPrePopulatedValues(10);
      simulatorConfiguration.setAmplitudesPrPackage(256);
      simulatorConfiguration.setStartTimeEpochSecond(0);
      simulatorConfiguration.afterInit();

      SimulatorBoxUnit simulatorBoxUnit = new SimulatorBoxUnit(simulatorConfiguration);

      CountDownLatch produced = new CountDownLatch(1);
      AtomicReference<Throwable> error = new AtomicReference<>();
      try {
        simulatorBoxUnit.produce()
          .subscribe(
            batch -> {
              for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry : batch) {
                kafkaRelay.relayToKafka(entry);
              }
            },
            ex -> {
              error.set(ex);
              produced.countDown();
            },
            produced::countDown
          );

        assertTrue(produced.await(60, TimeUnit.SECONDS), "Timed out waiting for producer to finish");
        Throwable producedError = error.get();
        assertNull(producedError, "Unexpected error from producer: " + producedError);
      } finally {
        kafkaRelay.teardown();
      }
    }

    long actual = countRecords(topic, expectedRecords, Duration.ofSeconds(60));
    assertEquals(expectedRecords, actual, "Unexpected record count in Kafka topic");
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  void routes64LociAcross8Partitions_with4Producers_andPreservesPartitionOrdering() throws Exception {
    String topic = "das-producer-it-" + UUID.randomUUID();
    int shots = 10;
    int loci = 64;
    int partitions = 8;
    int producerInstances = 4;
    long expectedRecords = (long) shots * (long) loci;

    createTopic(topic, partitions);
    purgeTopic(topic);

    String schemaRegistryUrl = "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);

    KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setTopic(topic);
    kafkaConfiguration.setPartitions(partitions);
    kafkaConfiguration.setProducerInstances(producerInstances);
    kafkaConfiguration.setRelayQueueCapacity(5_000);
    kafkaConfiguration.setRelayEnqueueTimeoutMillis(30_000);
    kafkaConfiguration.setRelayEnqueueWarnMillis(0);

    Map<String, Object> baseProps = kafkaConfiguration.kafkaProperties(KAFKA.getBootstrapServers(), schemaRegistryUrl);
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers = new ArrayList<>();
    for (int i = 0; i < producerInstances; i++) {
      Map<String, Object> props = new HashMap<>(baseProps);
      props.put(ProducerConfig.CLIENT_ID_CONFIG, "das-producer-it-" + i);
      producers.add(new KafkaProducer<>(props));
    }

    KafkaSender kafkaSender = new KafkaSender(new SimpleMeterRegistry());
    kafkaSender.setProducers(producers);

    DasProducerConfiguration dasProducerConfiguration = new DasProducerConfiguration();
    Map<Integer, Integer> locusToPartition = locusToModuloPartitionAssignments(loci, partitions);
    dasProducerConfiguration.setPartitionAssignments(locusToPartition);

    KafkaRelay kafkaRelay = new KafkaRelay(kafkaConfiguration, kafkaSender, dasProducerConfiguration);

    SimulatorBoxUnitConfiguration simulatorConfiguration = new SimulatorBoxUnitConfiguration();
    simulatorConfiguration.setAmplitudeDataType("long");
    simulatorConfiguration.setNumberOfShots(shots);
    simulatorConfiguration.setSecondsToRun(1);
    simulatorConfiguration.setPulseRate(10_000);
    simulatorConfiguration.setMaxFreq(10_000);
    simulatorConfiguration.setMinFreq(0);
    simulatorConfiguration.setBoxUUID("00000000-0000-0000-0000-000000000000");
    simulatorConfiguration.setOpticalPathUUID("00000000-0000-0000-0000-000000000000");
    simulatorConfiguration.setSpatialSamplingInterval(1.1f);
    simulatorConfiguration.setPulseWidth(100.5f);
    simulatorConfiguration.setStartLocusIndex(0);
    simulatorConfiguration.setDisableThrottling(true);
    simulatorConfiguration.setGaugeLength(10.209524f);
    simulatorConfiguration.setNumberOfLoci(loci);
    simulatorConfiguration.setNumberOfPrePopulatedValues(10);
    simulatorConfiguration.setAmplitudesPrPackage(256);
    simulatorConfiguration.setStartTimeEpochSecond(0);
    simulatorConfiguration.afterInit();

    SimulatorBoxUnit simulatorBoxUnit = new SimulatorBoxUnit(simulatorConfiguration);

    CountDownLatch produced = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();
    try {
      simulatorBoxUnit.produce()
        .subscribe(
          batch -> {
            for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry : batch) {
              kafkaRelay.relayToKafka(entry);
            }
          },
          ex -> {
            error.set(ex);
            produced.countDown();
          },
          produced::countDown
        );

      assertTrue(produced.await(90, TimeUnit.SECONDS), "Timed out waiting for producer to finish");
      Throwable producedError = error.get();
      assertNull(producedError, "Unexpected error from producer: " + producedError);
    } finally {
      kafkaRelay.teardown();
    }

    // Failures here can indicate production routing/ordering bugs, not just test flakiness.
    long actual = consumeAndAssertRoutingAndOrdering(
      topic,
      expectedRecords,
      Duration.ofSeconds(90),
      schemaRegistryUrl,
      locusToPartition
    );
    assertEquals(expectedRecords, actual, "Unexpected record count in Kafka topic");
  }

  private static Map<Integer, Integer> locusToSinglePartitionAssignments(int loci) {
    Map<Integer, Integer> map = new HashMap<>(loci);
    for (int locus = 0; locus < loci; locus++) {
      map.put(locus, 0);
    }
    return map;
  }

  private static Map<Integer, Integer> locusToModuloPartitionAssignments(int loci, int partitions) {
    Map<Integer, Integer> map = new HashMap<>(loci);
    for (int locus = 0; locus < loci; locus++) {
      map.put(locus, Math.floorMod(locus, partitions));
    }
    return map;
  }

  /**
   * Ensures the topic is empty before producing, so consecutive test runs do not accidentally
   * count old records (for example if the topic already existed).
   */
  private static void purgeTopic(String topic) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA.getBootstrapServers());
    try (AdminClient admin = AdminClient.create(props)) {
      List<TopicPartition> partitions = admin.describeTopics(List.of(topic))
        .allTopicNames()
        .get(30, TimeUnit.SECONDS)
        .get(topic)
        .partitions()
        .stream()
        .map(p -> new TopicPartition(topic, p.partition()))
        .collect(Collectors.toList());

      Map<TopicPartition, OffsetSpec> latestOffsetsSpec = new HashMap<>(partitions.size());
      for (TopicPartition tp : partitions) {
        latestOffsetsSpec.put(tp, OffsetSpec.latest());
      }

      Map<TopicPartition, Long> latestOffsets = admin.listOffsets(latestOffsetsSpec)
        .all()
        .get(30, TimeUnit.SECONDS)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

      Map<TopicPartition, RecordsToDelete> deleteBefore = new HashMap<>(partitions.size());
      for (Map.Entry<TopicPartition, Long> entry : latestOffsets.entrySet()) {
        long offset = entry.getValue() == null ? 0L : entry.getValue();
        deleteBefore.put(entry.getKey(), RecordsToDelete.beforeOffset(Math.max(0L, offset)));
      }

      DeleteRecordsResult result = admin.deleteRecords(deleteBefore);
      result.all().get(30, TimeUnit.SECONDS);
    }
  }

  private static void createTopic(String topic, int partitions) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA.getBootstrapServers());
    try (AdminClient admin = AdminClient.create(props)) {
      try {
        admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        Throwable cause = e.getCause();
        if (cause instanceof TopicExistsException) {
          return;
        }
        throw e;
      }
    }
  }

  private static long countRecords(String topic, long expected, Duration timeout) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "das-producer-it-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");

    long total = 0;
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      while (total < expected && System.nanoTime() < deadlineNanos) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        total += records.count();
      }
    }
    return total;
  }

  private static long consumeAndAssertRoutingAndOrdering(
    String topic,
    long expected,
    Duration timeout,
    String schemaRegistryUrl,
    Map<Integer, Integer> locusToPartition) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "das-producer-it-consumer-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");

    long total = 0;
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    Map<Integer, Integer> observedPartitionByLocus = new HashMap<>();
    Map<Integer, Long> lastTimestampByPartition = new HashMap<>();

    try (KafkaConsumer<DASMeasurementKey, DASMeasurement> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      while (total < expected && System.nanoTime() < deadlineNanos) {
        ConsumerRecords<DASMeasurementKey, DASMeasurement> records = consumer.poll(Duration.ofMillis(500));
        records.forEach(record -> {
          DASMeasurement value = record.value();
          assertNotNull(value, "Expected non-null DASMeasurement record");
          int locus = value.getLocus();
          int expectedPartition = locusToPartition.get(locus);
          assertEquals(expectedPartition, record.partition(), "Unexpected partition for locus " + locus);

          Integer previousPartition = observedPartitionByLocus.putIfAbsent(locus, record.partition());
          if (previousPartition != null) {
            assertEquals(previousPartition, record.partition(), "Locus mapped to multiple partitions");
          }

          long timestamp = value.getStartSnapshotTimeNano();
          Long previousTimestamp = lastTimestampByPartition.put(record.partition(), timestamp);
          if (previousTimestamp != null) {
            assertTrue(timestamp >= previousTimestamp,
              "Expected non-decreasing timestamps within partition " + record.partition());
          }
        });
        total += records.count();
      }
    }

    assertEquals(locusToPartition.keySet(), observedPartitionByLocus.keySet(), "Expected all loci to be observed");
    return total;
  }
}
