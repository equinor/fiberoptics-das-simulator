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

import com.equinor.fiberoptics.das.DasProducerFactory;
import com.equinor.fiberoptics.das.HttpUtils;
import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.GenericDasProducer;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnit;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnitConfiguration;
import com.equinor.fiberoptics.das.remotecontrol.RemoteControlController;
import com.equinor.fiberoptics.das.remotecontrol.RemoteControlService;
import com.equinor.fiberoptics.das.remotecontrol.profile.DasSimulatorProfileResolver;
import com.equinor.test.TestTimeouts;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.BindMode;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteControlSwitchAndStopIntegrationTest {

  private static final String CONFLUENT_VERSION = "8.1.1";
  private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:" + CONFLUENT_VERSION;
  private static final String STREAMINITIATOR_VERSION = readStreamInitiatorVersion();
  private static final String STREAMINITIATOR_IMAGE = "fibra/streaminitiator:" + STREAMINITIATOR_VERSION;
  private static final String KAFKA_KRAFT_CLUSTER_ID = "WjQ5NkZsUVFqY2x1Z0x4a1pPQQ";
  private static final int KAFKA_EXTERNAL_PORT = readKafkaExternalPort();
  private static final Duration CONTAINER_STARTUP_TIMEOUT = TestTimeouts.scaled(Duration.ofMinutes(3));
  private static final Duration TEST_TIMEOUT_LONG = TestTimeouts.scaled(Duration.ofMinutes(3));
  private static final Duration ADMIN_TIMEOUT = TestTimeouts.scaled(Duration.ofSeconds(30));
  private static final Duration CONSUMER_POLL_TIMEOUT = TestTimeouts.scaled(Duration.ofMillis(500));
  private static final Duration WAIT_UNTIL_TIMEOUT = TestTimeouts.scaled(Duration.ofSeconds(20));
  private static final Duration SHORT_SLEEP = TestTimeouts.scaled(Duration.ofMillis(100));
  private static final Duration RELAY_ENQUEUE_TIMEOUT = TestTimeouts.scaled(Duration.ofSeconds(30));
  private static final String INITIATOR_API_KEY = "1aa111a11aa11a0a1a1aa1111a1a1a1a";
  private static final Network NETWORK = Network.newNetwork();

  @SuppressWarnings({"resource", "deprecation"})
  private final FixedHostPortGenericContainer<?> KAFKA =
    new FixedHostPortGenericContainer<>(KAFKA_IMAGE)
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("KafkaContainer"))
        .withPrefix("kafka"))
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
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("SchemaRegistryContainer"))
        .withPrefix("schema-registry"))
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

  @SuppressWarnings("resource")
  private final GenericContainer<?> POSTGRES =
    new GenericContainer<>(DockerImageName.parse("postgres:17.7"))
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("PostgresContainer"))
        .withPrefix("postgres"))
      .withNetwork(NETWORK)
      .withNetworkAliases("db")
      .withEnv("POSTGRES_PASSWORD", "postgres")
      .withEnv("POSTGRES_USER", "postgres")
      .withEnv("POSTGRES_DB", "streamdb")
      .withExposedPorts(5432)
      .waitingFor(Wait.forListeningPort().withStartupTimeout(CONTAINER_STARTUP_TIMEOUT));

  @SuppressWarnings("resource")
  private final GenericContainer<?> STREAMINITIATOR =
    new GenericContainer<>(DockerImageName.parse(STREAMINITIATOR_IMAGE))
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("StreamInitiatorContainer"))
        .withPrefix("streaminitiator"))
      .dependsOn(POSTGRES, SCHEMA_REGISTRY)
      .withNetwork(NETWORK)
      .withNetworkAliases("streaminitiator")
      .withExposedPorts(8080)
      .withEnv("SERVER_PORT", "8080")
      .withEnv("BROKERS_COLD", "kafka:9092")
      .withEnv("BROKERS_INGRESS", "kafka:9092")
      .withEnv("ANNOUNCED_BROKERS_INGRESS", "kafka:9092")
      .withEnv("ZOOKEEPERS", "zookeeper:2181")
      .withEnv("SCHEMAREGISTRY", "http://schemaregistry:8081")
      .withEnv("ANNOUNCED_SCHEMAREGISTRY", "http://schemaregistry:8081")
      .withEnv("REPLICATION_FACTOR", "1")
      .withEnv("SPRING_PROFILES_ACTIVE", "docker")
      .withEnv("INTERROGATOR_ENV", "docker")
      .withEnv("PG_SUPERADMIN_URL", "jdbc:postgresql://db:5432/postgres?user=postgres&password=postgres&currentSchema=fibra")
      .withEnv("PG_APP_DB_URL", "jdbc:postgresql://db:5432/streamdb?user=postgres&password=postgres&currentSchema=fibra")
      .withEnv("MIGRATION_FOLDER", ",filesystem:/sql")
      .withEnv("SPRING_FLYWAY_OUT_OF_ORDER", "true")
      .withEnv("PARTITIONS", "5")
      .withEnv("PERFORMANCE_CEILING", "20000")
      .withEnv("INTERNAL_SERVICES_API_KEY", INITIATOR_API_KEY)
      .withEnv("VENDORS_KEYS_SIMULATOR_OLD", INITIATOR_API_KEY)
      .withEnv("FIBRA_CLUSTER", "demo")
      .withFileSystemBind(
        Path.of("dependson-services", "sql").toAbsolutePath().toString(),
        "/sql",
        BindMode.READ_ONLY
      )
      .waitingFor(
        Wait.forHttp("/actuator/health")
          .forPort(8080)
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

    try {
      POSTGRES.start();
    } catch (RuntimeException ex) {
      logContainerFailure("POSTGRES", POSTGRES);
      writeContainerLogs("postgres", POSTGRES);
      throw ex;
    }

    try {
      STREAMINITIATOR.start();
    } catch (RuntimeException ex) {
      logContainerFailure("STREAMINITIATOR", STREAMINITIATOR);
      writeContainerLogs("streaminitiator", STREAMINITIATOR);
      throw ex;
    }

    seedStreamInitiatorData();
  }

  @AfterAll
  void stopContainers() {
    writeContainerLogs("kafka", KAFKA);
    writeContainerLogs("schema-registry", SCHEMA_REGISTRY);
    writeContainerLogs("postgres", POSTGRES);
    writeContainerLogs("streaminitiator", STREAMINITIATOR);
    STREAMINITIATOR.stop();
    POSTGRES.stop();
    SCHEMA_REGISTRY.stop();
    KAFKA.stop();
    NETWORK.close();
  }

  private String streamInitiatorUrl() {
    return "http://" + STREAMINITIATOR.getHost() + ":" + STREAMINITIATOR.getMappedPort(8080);
  }

  private String kafkaBootstrapServers() {
    return KAFKA.getHost() + ":" + KAFKA_EXTERNAL_PORT;
  }

  private static int readKafkaExternalPort() {
    String value = System.getenv("TEST_KAFKA_EXTERNAL_PORT");
    if (value == null || value.isBlank()) {
      return findAvailablePort();
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ex) {
      throw new IllegalStateException("TEST_KAFKA_EXTERNAL_PORT must be an integer, got: " + value, ex);
    }
  }

  private static int findAvailablePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    } catch (IOException ex) {
      throw new IllegalStateException("Unable to find a free port for Kafka", ex);
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
    } catch (RuntimeException | IOException logEx) {
      System.err.println("Failed to write " + name + " logs: " + logEx.getMessage());
    }
  }

  private void seedStreamInitiatorData() {
    String sql = """
      INSERT INTO fibra.vendor (id, name, vendor_code)
      VALUES ('3a1e813f-a272-498c-8850-6188587369cd', 'Simulator vendor', 'simulator')
      ON CONFLICT (id) DO NOTHING;

      INSERT INTO fibra.instrument_box (
        id,
        description,
        data_format,
        measurement_unit,
        de_noised,
        native_to_phase_scaling_factor,
        phase_to_strain_scaling_factor,
        instrument_box_info,
        vendor_id
      )
      VALUES (
        '00528e45-06d0-4110-bba4-e904aaa02657',
        'Simulator 1',
        'amplitude',
        'ns',
        false,
        1.0,
        1.0,
        '{}',
        '3a1e813f-a272-498c-8850-6188587369cd'
      )
      ON CONFLICT (id) DO NOTHING;

      INSERT INTO fibra.instrument_box (
        id,
        description,
        data_format,
        measurement_unit,
        de_noised,
        native_to_phase_scaling_factor,
        phase_to_strain_scaling_factor,
        instrument_box_info,
        vendor_id
      )
      VALUES (
        '1deb5d57-2fb9-418a-990b-4cf7252a0450',
        'Simulator 2',
        'amplitude',
        'ns',
        false,
        1.0,
        1.0,
        '{}',
        '3a1e813f-a272-498c-8850-6188587369cd'
      )
      ON CONFLICT (id) DO NOTHING;

      INSERT INTO fibra.fiber_optical_path (id, wellbore, switch_port, fiber_optical_path_info)
      VALUES ('25be8bc6-cd35-4c9f-9f8d-cabe43326163', 'Simulator box fiber path', 0, '{}')
      ON CONFLICT (id) DO NOTHING;
      """;
    try {
      ExecResult result = POSTGRES.execInContainer(
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "postgres",
        "-d",
        "streamdb",
        "-c",
        sql
      );
      if (result.getExitCode() != 0) {
        throw new IllegalStateException(
          "Seed SQL failed: " + result.getStderr() + " " + result.getStdout()
        );
      }
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to seed streaminitiator data", ex);
    }
  }

  @Test
  void remoteControl_canApplySwitchConfiguration_thenStop(@TempDir Path tempDir) throws Exception {
    assertTimeoutPreemptively(TEST_TIMEOUT_LONG, () -> {
    String schemaRegistryUrl = "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    String apiKey = "test-api-key";

    writeProfile(tempDir, "profile-one", 4, "00528e45-06d0-4110-bba4-e904aaa02657");
    writeProfile(tempDir, "profile-two", 7, "1deb5d57-2fb9-418a-990b-4cf7252a0450");

      DasProducerConfiguration dasProducerConfiguration = new DasProducerConfiguration();
      dasProducerConfiguration.setVariant("SimulatorBoxUnit");
      dasProducerConfiguration.setVendorCode("simulator");
      dasProducerConfiguration.setAcquisitionStartVersion("V1");
      dasProducerConfiguration.setInitiatorserviceUrl(streamInitiatorUrl());
      dasProducerConfiguration.setInitiatorserviceApiKey(INITIATOR_API_KEY);
      dasProducerConfiguration.setOverrideBootstrapServersWith(kafkaBootstrapServers());
      dasProducerConfiguration.setOverrideSchemaRegistryWith(schemaRegistryUrl);
      dasProducerConfiguration.getRemoteControl().setEnabled(true);
      dasProducerConfiguration.getRemoteControl().setApiKey(apiKey);
      dasProducerConfiguration.getRemoteControl().setProfilesDirectory(tempDir.toString());

      KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
      kafkaConfiguration.setProducerInstances(1);
      kafkaConfiguration.setRelayQueueCapacity(5_000);
      kafkaConfiguration.setRelayEnqueueTimeoutMillis(RELAY_ENQUEUE_TIMEOUT.toMillis());
      kafkaConfiguration.setRelayEnqueueWarnMillis(0);

      SimulatorBoxUnitConfiguration simulatorConfiguration = new SimulatorBoxUnitConfiguration();
      simulatorConfiguration.setAmplitudeDataType("long");
      simulatorConfiguration.setSecondsToRun(600);
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
      simulatorConfiguration.setNumberOfLoci(4);
      simulatorConfiguration.setNumberOfPrePopulatedValues(10);
      simulatorConfiguration.setAmplitudesPrPackage(256);
      simulatorConfiguration.setStartTimeEpochSecond(0);
      simulatorConfiguration.afterInit();

      BeanFactory beanFactory = mock(BeanFactory.class);
      when(beanFactory.getBean(eq("SimulatorBoxUnit"), eq(GenericDasProducer.class)))
        .thenAnswer(invocation -> new SimulatorBoxUnit(simulatorConfiguration));

      ApplicationContext applicationContext = mock(ApplicationContext.class);
      KafkaSender kafkaSender = new KafkaSender(new SimpleMeterRegistry(), kafkaConfiguration);
      KafkaRelay kafkaRelay = new KafkaRelay(kafkaConfiguration, kafkaSender, dasProducerConfiguration);

      HttpUtils httpUtils = newHttpUtils(simulatorConfiguration, dasProducerConfiguration);
      DasProducerFactory dasProducerFactory = newDasProducerFactory(
        kafkaConfiguration, httpUtils, dasProducerConfiguration, kafkaSender, applicationContext);

      ObjectMapper objectMapper = new ObjectMapper();
      DasSimulatorProfileResolver profileResolver = new DasSimulatorProfileResolver(dasProducerConfiguration, objectMapper);
      RemoteControlService remoteControlService = new RemoteControlService(
        beanFactory,
        dasProducerConfiguration,
        simulatorConfiguration,
        dasProducerFactory,
        kafkaRelay,
        kafkaSender,
        objectMapper,
        profileResolver);
      RemoteControlController remoteControlController = new RemoteControlController(remoteControlService, dasProducerConfiguration);
      MockMvc mvc = MockMvcBuilders.standaloneSetup(remoteControlController).build();

      mvc.perform(post("/api/acquisition/apply")
          .header("X-Api-Key", apiKey)
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"Custom\":{\"das-simulator-profile\":\"profile-one\"}}"))
        .andExpect(status().isOk());

      mvc.perform(post("/api/acquisition/apply")
          .header("X-Api-Key", apiKey)
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"Custom\":{\"das-simulator-profile\":\"profile-two\"}}"))
        .andExpect(status().isOk());

      mvc.perform(post("/api/acquisition/stop")
          .header("X-Api-Key", apiKey)
          .contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

      AtomicReference<String> topicRef = new AtomicReference<>();
      waitUntil("topic assignment", WAIT_UNTIL_TIMEOUT, () -> {
        String topic = kafkaConfiguration.getTopic();
        if (topic == null || topic.isBlank()) {
          return false;
        }
        topicRef.set(topic);
        return true;
      });

      String topic = topicRef.get();
      long observed = countRecords(topic, 1, WAIT_UNTIL_TIMEOUT);
      assertTrue(observed > 0, "Expected Kafka topic to contain records after APPLY calls");
    });
  }

  private static HttpUtils newHttpUtils(
    SimulatorBoxUnitConfiguration simulatorConfiguration,
    DasProducerConfiguration dasProducerConfiguration) {
    try {
      Constructor<HttpUtils> constructor = HttpUtils.class.getDeclaredConstructor(
        SimulatorBoxUnitConfiguration.class,
        DasProducerConfiguration.class,
        RestTemplateBuilder.class);
      constructor.setAccessible(true);
      return constructor.newInstance(
          simulatorConfiguration,
          dasProducerConfiguration,
          new RestTemplateBuilder()
      );
    } catch (Exception e) {
      throw new IllegalStateException("Unable to create HttpUtils for integration test", e);
    }
  }

  private static DasProducerFactory newDasProducerFactory(
    KafkaConfiguration kafkaConfiguration,
    HttpUtils httpUtils,
    DasProducerConfiguration dasProducerConfiguration,
    KafkaSender kafkaSender,
    ApplicationContext applicationContext) {
    try {
      Constructor<DasProducerFactory> constructor = DasProducerFactory.class.getDeclaredConstructor(
        KafkaConfiguration.class,
        HttpUtils.class,
        DasProducerConfiguration.class,
        KafkaSender.class,
        ApplicationContext.class);
      constructor.setAccessible(true);
      return constructor.newInstance(
        kafkaConfiguration,
        httpUtils,
        dasProducerConfiguration,
        kafkaSender,
        applicationContext);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to create DasProducerFactory for integration test", e);
    }
  }

  private static void waitUntil(String description, Duration timeout, BooleanSupplier condition) throws Exception {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(Math.max(1L, SHORT_SLEEP.toMillis()));
    }
    throw new AssertionError("Timed out waiting for: " + description);
  }

  private static void writeProfile(Path profilesDir, String profileId, int loci, String boxUuid) throws Exception {
    String json = """
      {
        "NumberOfLoci": %d,
        "StartLocusIndex": 0,
        "PulseRate": 10000,
        "MaximumFrequency": 5000,
        "SpatialSamplingInterval": 1.1,
        "GaugeLength": 10.209524,
        "PulseWidth": 100.50,
        "PulseWidthUnit": "ns",
        "DasInstrumentBoxUUID": "%s",
        "OpticalPathUUID": "25be8bc6-cd35-4c9f-9f8d-cabe43326163",
        "VendorCode": "simulator",
        "AcquisitionId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "MeasurementStartTime": "2026-01-27T08:56:35Z"
      }
      """.formatted(loci, boxUuid).trim();
    Files.writeString(profilesDir.resolve(profileId + ".json"), json, StandardCharsets.UTF_8);
  }

  private long countRecords(String topic, long expected, Duration timeout) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "das-remote-it-consumer-" + UUID.randomUUID());
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


  private static String readStreamInitiatorVersion() {
    String value = System.getenv("STREAMINITIATOR_VERSION");
    if (value == null || value.isBlank()) {
      return "1.8.75";
    }
    return value.trim();
  }
}
