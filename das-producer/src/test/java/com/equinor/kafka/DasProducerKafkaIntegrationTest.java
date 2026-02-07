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
import com.equinor.test.TestTimeouts;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DasProducerKafkaIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(DasProducerKafkaIntegrationTest.class);
  private static final String CONFLUENT_VERSION = "8.1.1";
  private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:" + CONFLUENT_VERSION;
  private static final String KAFKA_KRAFT_CLUSTER_ID = "WjQ5NkZsUVFqY2x1Z0x4a1pPQQ";
  private static final int KAFKA_EXTERNAL_PORT = readKafkaExternalPort();
  private static final Duration CONTAINER_STARTUP_TIMEOUT = TestTimeouts.scaled(Duration.ofMinutes(3));
  private static final Duration TEST_TIMEOUT_SHORT = TestTimeouts.scaled(Duration.ofMinutes(2));
  private static final Duration TEST_TIMEOUT_LONG = TestTimeouts.scaled(Duration.ofMinutes(3));
  private static final Duration PRODUCER_FINISH_TIMEOUT = TestTimeouts.scaled(Duration.ofSeconds(60));
  private static final Duration PRODUCER_FINISH_LONG_TIMEOUT = TestTimeouts.scaled(Duration.ofSeconds(90));
  private static final Duration COUNT_RECORDS_TIMEOUT = TestTimeouts.scaled(Duration.ofSeconds(60));
  private static final Duration COUNT_RECORDS_LONG_TIMEOUT = TestTimeouts.scaled(Duration.ofSeconds(90));
  private static final Duration ADMIN_TIMEOUT = TestTimeouts.scaled(Duration.ofSeconds(30));
  private static final Duration CONSUMER_POLL_TIMEOUT = TestTimeouts.scaled(Duration.ofMillis(500));
  private static final Duration RELAY_ENQUEUE_TIMEOUT = TestTimeouts.scaled(Duration.ofSeconds(30));
  private static final Network NETWORK = Network.newNetwork();

  @SuppressWarnings({"resource", "deprecation"})
  private final FixedHostPortGenericContainer<?> KAFKA =
    new FixedHostPortGenericContainer<>(KAFKA_IMAGE)
      .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("kafka"))
      .withNetwork(NETWORK)
      .withNetworkAliases("kafka")
      .withCommand("/etc/confluent/docker/run")
      .withExposedPorts(9092)
      .withFixedExposedPort(KAFKA_EXTERNAL_PORT, 29094)
      .withEnv("KAFKA_NODE_ID", "1")
      .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
      .withEnv("KAFKA_LISTENERS", "INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29094,CONTROLLER://0.0.0.0:9093")
      .withEnv("KAFKA_ADVERTISED_LISTENERS", "INTERNAL://kafka:9092,EXTERNAL://localhost:" + KAFKA_EXTERNAL_PORT)
      .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
      .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093")
      .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
      .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
      .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
      .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
      .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
      .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
      .withEnv("KAFKA_KRAFT_CLUSTER_ID", KAFKA_KRAFT_CLUSTER_ID)
      .withEnv("KAFKA_CLUSTER_ID", KAFKA_KRAFT_CLUSTER_ID)
      .withEnv("CLUSTER_ID", KAFKA_KRAFT_CLUSTER_ID)
      .waitingFor(Wait.forListeningPort().withStartupTimeout(CONTAINER_STARTUP_TIMEOUT));

  @SuppressWarnings("resource")
  private final GenericContainer<?> SCHEMA_REGISTRY =
    new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:" + CONFLUENT_VERSION))
      .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("schema-registry"))
      .dependsOn(KAFKA)
      .withNetwork(NETWORK)
      .withNetworkAliases("schemaregistry")
      .withExposedPorts(8081)
      .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schemaregistry")
      .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
      .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
      .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "PLAINTEXT")
      .waitingFor(
        Wait.forHttp("/subjects")
          .forPort(8081)
          .forStatusCode(200)
            .withStartupTimeout(CONTAINER_STARTUP_TIMEOUT)
      );

  @BeforeAll
  void startContainers() {
    try {
      KAFKA.start();
    } catch (RuntimeException ex) {
      logContainerFailure("KAFKA", KAFKA);
      writeContainerLogs("kafka", KAFKA);
      throw ex;
    }

    try {
      SCHEMA_REGISTRY.start();
    } catch (RuntimeException ex) {
      logContainerFailure("SCHEMA_REGISTRY", SCHEMA_REGISTRY);
      writeContainerLogs("schema-registry", SCHEMA_REGISTRY);
      throw ex;
    }
  }

  @AfterAll
  void stopContainers() {
    writeContainerLogs("kafka", KAFKA);
    writeContainerLogs("schema-registry", SCHEMA_REGISTRY);
    SCHEMA_REGISTRY.stop();
    KAFKA.stop();
    NETWORK.close();
  }

  private String kafkaBootstrapServers() {
    return KAFKA.getHost() + ":" + KAFKA_EXTERNAL_PORT;
  }

  private static int readKafkaExternalPort() {
    String value = System.getenv("TEST_KAFKA_EXTERNAL_PORT");
    if (value == null || value.isBlank()) {
      return 29094;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ex) {
      throw new IllegalStateException("TEST_KAFKA_EXTERNAL_PORT must be an integer, got: " + value, ex);
    }
  }

  private void logContainerFailure(String name, GenericContainer<?> container) {
    try {
      String containerId = container.getContainerId();
      System.err.println("Container " + name + " failed to start. id=" + containerId);
      System.err.println(container.getLogs());
    } catch (RuntimeException logEx) {
      System.err.println("Container " + name + " failed to start, and logs could not be read: " + logEx.getMessage());
    }
  }

  private void writeContainerLogs(String name, GenericContainer<?> container) {
    try {
      Path logPath = Path.of("target", "testcontainers", name + ".log");
      Files.createDirectories(logPath.getParent());
      Files.writeString(logPath, container.getLogs(), StandardCharsets.UTF_8);
      System.err.println("Wrote " + name + " logs to " + logPath);
    } catch (RuntimeException | java.io.IOException logEx) {
      System.err.println("Failed to write " + name + " logs: " + logEx.getMessage());
    }
  }

  @Test
  void produces50ShotsWith500Loci_at10kHz_andWritesExpectedKafkaRecordCount() throws Exception {
    assertTimeoutPreemptively(TEST_TIMEOUT_SHORT, () -> {
    String topic = "das-producer-it-" + UUID.randomUUID();
    int shots = 50;
    int loci = 500;
    long expectedRecords = (long) shots * (long) loci;

    createTopic(topic, 1);
    purgeTopic(topic);

    String schemaRegistryUrl = "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);

    logger.info(
      "DAS producer integration-test configuration: topic={}, shots={}, loci={}, pulseRateHz={}, maxFreqHz={}, amplitudesPrPackage={}, amplitudeDataType={}, disableThrottling={}, expectedRecords={}, bootstrapServers={}, schemaRegistryUrl={}",
      topic, shots, loci, 10_000, 10_000, 256, "long", true, expectedRecords, kafkaBootstrapServers(), schemaRegistryUrl
    );
    logger.info("Note: this test runs the producer implementation directly (no Spring Boot app), so StartupInfoLogger is not invoked.");

    KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setTopic(topic);
    kafkaConfiguration.setPartitions(1);
    kafkaConfiguration.setProducerInstances(1);
    kafkaConfiguration.setRelayQueueCapacity(5_000);
    kafkaConfiguration.setRelayEnqueueTimeoutMillis(RELAY_ENQUEUE_TIMEOUT.toMillis());
    kafkaConfiguration.setRelayEnqueueWarnMillis(0);

    Map<String, Object> producerProps = kafkaConfiguration.kafkaProperties(kafkaBootstrapServers(), schemaRegistryUrl);
    try (KafkaProducer<DASMeasurementKey, DASMeasurement> kafkaProducer = new KafkaProducer<>(producerProps)) {
      KafkaSender kafkaSender = new KafkaSender(new SimpleMeterRegistry(), kafkaConfiguration);
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

        assertTrue(
          produced.await(PRODUCER_FINISH_TIMEOUT.toSeconds(), TimeUnit.SECONDS),
          "Timed out waiting for producer to finish"
        );
        Throwable producedError = error.get();
        assertNull(producedError, "Unexpected error from producer: " + producedError);
      } finally {
        kafkaRelay.teardown();
      }
    }

    long actual = countRecords(topic, expectedRecords, COUNT_RECORDS_TIMEOUT);
    assertEquals(expectedRecords, actual, "Unexpected record count in Kafka topic");
    });
  }

  @Test
  void routes64LociAcross8Partitions_with4Producers_andPreservesPartitionOrdering() throws Exception {
    assertTimeoutPreemptively(TEST_TIMEOUT_LONG, () -> {
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
    kafkaConfiguration.setRelayEnqueueTimeoutMillis(RELAY_ENQUEUE_TIMEOUT.toMillis());
    kafkaConfiguration.setRelayEnqueueWarnMillis(0);

    Map<String, Object> baseProps = kafkaConfiguration.kafkaProperties(kafkaBootstrapServers(), schemaRegistryUrl);
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers = new ArrayList<>();
    for (int i = 0; i < producerInstances; i++) {
      Map<String, Object> props = new HashMap<>(baseProps);
      props.put(ProducerConfig.CLIENT_ID_CONFIG, "das-producer-it-" + i);
      producers.add(new KafkaProducer<>(props));
    }

    KafkaSender kafkaSender = new KafkaSender(new SimpleMeterRegistry(), kafkaConfiguration);
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

      assertTrue(
        produced.await(PRODUCER_FINISH_LONG_TIMEOUT.toSeconds(), TimeUnit.SECONDS),
        "Timed out waiting for producer to finish"
      );
      Throwable producedError = error.get();
      assertNull(producedError, "Unexpected error from producer: " + producedError);
    } finally {
      kafkaRelay.teardown();
    }

    // Failures here can indicate production routing/ordering bugs, not just test flakiness.
    long actual = consumeAndAssertRoutingAndOrdering(
      topic,
      expectedRecords,
      COUNT_RECORDS_LONG_TIMEOUT,
      schemaRegistryUrl,
      locusToPartition
    );
    assertEquals(expectedRecords, actual, "Unexpected record count in Kafka topic");
    });
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
  private void purgeTopic(String topic) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBootstrapServers());
    try (AdminClient admin = AdminClient.create(props)) {
      List<TopicPartition> partitions = admin.describeTopics(List.of(topic))
        .allTopicNames()
        .get(ADMIN_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
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
        .get(ADMIN_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

      Map<TopicPartition, RecordsToDelete> deleteBefore = new HashMap<>(partitions.size());
      for (Map.Entry<TopicPartition, Long> entry : latestOffsets.entrySet()) {
        long offset = entry.getValue() == null ? 0L : entry.getValue();
        deleteBefore.put(entry.getKey(), RecordsToDelete.beforeOffset(Math.max(0L, offset)));
      }

      DeleteRecordsResult result = admin.deleteRecords(deleteBefore);
      result.all().get(ADMIN_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }
  }

  private void createTopic(String topic, int partitions) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBootstrapServers());
    try (AdminClient admin = AdminClient.create(props)) {
      try {
        admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all()
          .get(ADMIN_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      } catch (Exception e) {
        Throwable cause = e.getCause();
        if (cause instanceof TopicExistsException) {
          return;
        }
        throw e;
      }
    }
  }

  private long countRecords(String topic, long expected, Duration timeout) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
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
        ConsumerRecords<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_TIMEOUT);
        total += records.count();
      }
    }
    return total;
  }

  private long consumeAndAssertRoutingAndOrdering(
    String topic,
    long expected,
    Duration timeout,
    String schemaRegistryUrl,
    Map<Integer, Integer> locusToPartition) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
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
        ConsumerRecords<DASMeasurementKey, DASMeasurement> records = consumer.poll(CONSUMER_POLL_TIMEOUT);
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
