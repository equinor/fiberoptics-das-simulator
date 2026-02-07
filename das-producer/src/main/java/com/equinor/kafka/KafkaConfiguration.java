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

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the Kafka Producer. The config elements that directly relates to Kafka can
 * also be found here <a href="http://kafka.apache.org/documentation.html#producerconfigs">Kafka
 * documentation</a>
 *
 * @author Espen Tjønneland - espen@tjonneland.no
 */
@ConfigurationProperties(prefix = "das.producer.kafka")
public class KafkaConfiguration {

  private Map<String, String> config = new HashMap<>();
  private String topic;
  private int partitions;
  /**
   * Number of KafkaProducer instances to use in this process.
   *
   * <p>Each producer has its own buffer and background IO, which can improve throughput in
   * high-volume scenarios. Partitions are deterministically mapped to a producer to preserve
   * per-partition ordering.
   */
  private int producerInstances = 2;
  /**
   * Max number of pending send tasks per Kafka partition in {@link KafkaRelay}.
   *
   * <p>This is an application-level backpressure safeguard. When the queue is full, producers
   * will block (or time out) instead of allowing unbounded memory growth.
   */
  private int relayQueueCapacity = 100;

  /**
   * How long {@link KafkaRelay} should block waiting for space in the per-partition queue when
   * full. A value {@code <= 0} blocks indefinitely.
   */
  private long relayEnqueueTimeoutMillis = 0;

  /**
   * Emit a warning log when enqueueing a send task blocks for longer than this threshold.
   */
  private long relayEnqueueWarnMillis = 1000;

  public Map<String, Object> kafkaProperties(String bootstrapServers, String schemaRegistry) {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    mergeProps(props, config);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    return props;
  }

  private void mergeProps(Map<String, Object> props, Map<String, String> config) {
    config.forEach(props::put);
  }

  public void setConfig(Map<String, String> config) {
    if (config == null) {
      this.config = new HashMap<>();
    } else {
      this.config = new HashMap<>(config);
    }
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public int getPartitions() {
    return partitions;
  }

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public int getProducerInstances() {
    return producerInstances;
  }

  public void setProducerInstances(int producerInstances) {
    this.producerInstances = producerInstances;
  }

  public int getRelayQueueCapacity() {
    return relayQueueCapacity;
  }

  public void setRelayQueueCapacity(int relayQueueCapacity) {
    this.relayQueueCapacity = relayQueueCapacity;
  }

  public long getRelayEnqueueTimeoutMillis() {
    return relayEnqueueTimeoutMillis;
  }

  public void setRelayEnqueueTimeoutMillis(long relayEnqueueTimeoutMillis) {
    this.relayEnqueueTimeoutMillis = relayEnqueueTimeoutMillis;
  }

  public long getRelayEnqueueWarnMillis() {
    return relayEnqueueWarnMillis;
  }

  public void setRelayEnqueueWarnMillis(long relayEnqueueWarnMillis) {
    this.relayEnqueueWarnMillis = relayEnqueueWarnMillis;
  }
}
