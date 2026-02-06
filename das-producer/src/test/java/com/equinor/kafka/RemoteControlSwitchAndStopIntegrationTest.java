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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers(disabledWithoutDocker = true)
class RemoteControlSwitchAndStopIntegrationTest {

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
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  void remoteControl_canApplySwitchConfiguration_thenStop(@TempDir Path tempDir) throws Exception {
    String topic = "das-remote-it-" + UUID.randomUUID();
    String schemaRegistryUrl = "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    String apiKey = "test-api-key";

    createTopic(topic, 1);
    purgeTopic(topic);

    writeProfile(tempDir, "profile-one", 4, "11111111-1111-1111-1111-111111111111");
    writeProfile(tempDir, "profile-two", 7, "22222222-2222-2222-2222-222222222222");

    try (InitiatorStubServer initiator = InitiatorStubServer.start(topic, 1)) {
      DasProducerConfiguration dasProducerConfiguration = new DasProducerConfiguration();
      dasProducerConfiguration.setVariant("SimulatorBoxUnit");
      dasProducerConfiguration.setVendorCode("Simulator");
      dasProducerConfiguration.setAcquisitionStartVersion("V1");
      dasProducerConfiguration.setInitiatorserviceUrl(initiator.baseUrl());
      dasProducerConfiguration.setInitiatorserviceApiKey("initiator-api-key");
      dasProducerConfiguration.setOverrideBootstrapServersWith(KAFKA.getBootstrapServers());
      dasProducerConfiguration.setOverrideSchemaRegistryWith(schemaRegistryUrl);
      dasProducerConfiguration.getRemoteControl().setEnabled(true);
      dasProducerConfiguration.getRemoteControl().setApiKey(apiKey);
      dasProducerConfiguration.getRemoteControl().setProfilesDirectory(tempDir.toString());

      KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
      kafkaConfiguration.setProducerInstances(1);
      kafkaConfiguration.setRelayQueueCapacity(5_000);
      kafkaConfiguration.setRelayEnqueueTimeoutMillis(30_000);
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
      KafkaSender kafkaSender = new KafkaSender(new SimpleMeterRegistry());
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

      waitUntil("first start-acquisition call", Duration.ofSeconds(20), () -> initiator.startCallCount() >= 1);

      mvc.perform(post("/api/acquisition/apply")
          .header("X-Api-Key", apiKey)
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"Custom\":{\"das-simulator-profile\":\"profile-two\"}}"))
        .andExpect(status().isOk());

      waitUntil("second start-acquisition call", Duration.ofSeconds(20), () -> initiator.startCallCount() >= 2);
      waitUntil("switch stop-acquisition call", Duration.ofSeconds(20), () -> initiator.stopCallCount() >= 1);

      List<Integer> startedLoci = initiator.startedLoci();
      assertEquals(List.of(4, 7), startedLoci, "Expected switch from first profile loci to second profile loci");

      String firstAcquisitionId = initiator.startedAcquisitionIds().get(0);
      String secondAcquisitionId = initiator.startedAcquisitionIds().get(1);
      assertTrue(initiator.stoppedAcquisitionIds().contains(firstAcquisitionId),
        "Expected switch to stop previous acquisition");

      mvc.perform(post("/api/acquisition/stop")
          .header("X-Api-Key", apiKey)
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"AcquisitionId\":\"" + secondAcquisitionId + "\"}"))
        .andExpect(status().isOk());

      waitUntil("final stop-acquisition call", Duration.ofSeconds(20), () -> initiator.stopCallCount() >= 2);
      assertTrue(initiator.stoppedAcquisitionIds().contains(secondAcquisitionId),
        "Expected explicit STOP to stop current acquisition");

      long observed = countRecords(topic, 1, Duration.ofSeconds(20));
      assertTrue(observed > 0, "Expected Kafka topic to contain records after APPLY calls");
    }
  }

  private static HttpUtils newHttpUtils(
    SimulatorBoxUnitConfiguration simulatorConfiguration,
    DasProducerConfiguration dasProducerConfiguration) {
    try {
      Constructor<HttpUtils> constructor = HttpUtils.class.getDeclaredConstructor(
        SimulatorBoxUnitConfiguration.class,
        DasProducerConfiguration.class);
      constructor.setAccessible(true);
      return constructor.newInstance(simulatorConfiguration, dasProducerConfiguration);
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
      Thread.sleep(100);
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
        "DasInstrumentBoxUUID": "%s",
        "OpticalPathUUID": "9f79c244-1fec-4c78-83f9-e4b001f1c40f",
        "VendorCode": "Simulator",
        "AcquisitionId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "MeasurementStartTime": "2026-01-27T08:56:35Z"
      }
      """.formatted(loci, boxUuid).trim();
    Files.writeString(profilesDir.resolve(profileId + ".json"), json, StandardCharsets.UTF_8);
  }

  private static long countRecords(String topic, long expected, Duration timeout) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
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
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        total += records.count();
      }
    }
    return total;
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

  private static final class InitiatorStubServer implements AutoCloseable {
    private final HttpServer server;
    private final String topic;
    private final int partitions;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<Integer> startedLoci = new CopyOnWriteArrayList<>();
    private final List<String> startedAcquisitionIds = new CopyOnWriteArrayList<>();
    private final List<String> stoppedAcquisitionIds = new CopyOnWriteArrayList<>();
    private final AtomicInteger startCallCount = new AtomicInteger();
    private final AtomicInteger stopCallCount = new AtomicInteger();

    private InitiatorStubServer(HttpServer server, String topic, int partitions) {
      this.server = server;
      this.topic = topic;
      this.partitions = partitions;
      registerContexts();
      this.server.start();
    }

    static InitiatorStubServer start(String topic, int partitions) throws Exception {
      HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      return new InitiatorStubServer(server, topic, partitions);
    }

    String baseUrl() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    int startCallCount() {
      return startCallCount.get();
    }

    int stopCallCount() {
      return stopCallCount.get();
    }

    List<Integer> startedLoci() {
      return new ArrayList<>(startedLoci);
    }

    List<String> startedAcquisitionIds() {
      return new ArrayList<>(startedAcquisitionIds);
    }

    List<String> stoppedAcquisitionIds() {
      return new ArrayList<>(stoppedAcquisitionIds);
    }

    @Override
    public void close() {
      server.stop(0);
    }

    private void registerContexts() {
      server.createContext("/actuator/health", exchange -> {
        if (!"GET".equals(exchange.getRequestMethod())) {
          sendText(exchange, 405, "Method Not Allowed");
          return;
        }
        sendJson(exchange, 200, "{\"status\":\"UP\"}");
      });

      server.createContext("/api/acquisition/start", exchange -> {
        if (!"POST".equals(exchange.getRequestMethod())) {
          sendText(exchange, 405, "Method Not Allowed");
          return;
        }
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        JsonNode payload = objectMapper.readTree(body);
        int loci = payload.path("NumberOfLoci").asInt();
        String acquisitionId = payload.path("AcquisitionId").asText("");
        startedLoci.add(loci);
        startedAcquisitionIds.add(acquisitionId);
        startCallCount.incrementAndGet();

        ObjectNode response = objectMapper.createObjectNode();
        response.put("topic", topic);
        response.put("bootstrapServers", "ignored:9092");
        response.put("schemaRegistryUrl", "http://ignored:8081");
        response.put("numberOfPartitions", partitions);
        ObjectNode assignments = response.putObject("partitionAssignments");
        for (int locus = 0; locus < loci; locus++) {
          assignments.put(String.valueOf(locus), locus % partitions);
        }
        sendJson(exchange, 200, response.toString());
      });

      server.createContext("/api/v1/acquisition/stop", exchange -> {
        if (!"POST".equals(exchange.getRequestMethod())) {
          sendText(exchange, 405, "Method Not Allowed");
          return;
        }
        String path = exchange.getRequestURI().getPath();
        String acquisitionId = path.substring(path.lastIndexOf('/') + 1);
        stoppedAcquisitionIds.add(acquisitionId);
        stopCallCount.incrementAndGet();
        sendJson(exchange, 200, "{}");
      });
    }

    private void sendJson(HttpExchange exchange, int statusCode, String body) throws IOException {
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(statusCode, bytes.length);
      exchange.getResponseBody().write(bytes);
      exchange.close();
    }

    private void sendText(HttpExchange exchange, int statusCode, String body) throws IOException {
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(statusCode, bytes.length);
      exchange.getResponseBody().write(bytes);
      exchange.close();
    }
  }
}
