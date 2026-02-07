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

import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import io.micrometer.core.instrument.MeterRegistry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Sends measurements to Kafka using a managed producer set.
 */
@Component
public class KafkaSender {

  private final AtomicReference<List<KafkaProducer<DASMeasurementKey, DASMeasurement>>>
      _producersRef = new AtomicReference<>();
  private final MeterRegistry _meterRegistry;

  private final AtomicBoolean _isRunning = new AtomicBoolean(true);

  private static final Logger _logger = LoggerFactory.getLogger(KafkaSender.class);

  /**
   * Creates a sender that reports metrics to the provided registry.
   */
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP2",
      justification = "MeterRegistry is managed by Spring and shared."
  )
  public KafkaSender(MeterRegistry meterRegistry) {
    _meterRegistry = meterRegistry;
  }

  /**
   * Sets a single producer.
   */
  public void setProducer(KafkaProducer<DASMeasurementKey, DASMeasurement> producer) {
    if (producer == null) {
      return;
    }
    setProducers(List.of(producer));
  }

  /**
   * Replaces the current producer set.
   */
  public void setProducers(List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers) {
    if (producers == null || producers.isEmpty()
        || producers.stream().anyMatch(Objects::isNull)) {
      return;
    }
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> previous =
        _producersRef.getAndSet(List.copyOf(producers));
    closeProducers(previous);
    _isRunning.set(true);
  }

  /**
   * Sends a record to Kafka.
   */
  public void send(ProducerRecord<DASMeasurementKey, DASMeasurement> data) {
    if (!_isRunning.get()) {
      // logger.info("Producer not running");
      return;
    }
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers = _producersRef.get();
    if (producers == null || producers.isEmpty()) {
      return;
    }
    KafkaProducer<DASMeasurementKey, DASMeasurement> producer =
        producerForPartition(producers, data.partition());

    Headers headers = data.headers();
    headers.add(
        KafkaHeaders.TIMESTAMP,
        longToBytes(data.value().getStartSnapshotTimeNano() / 1_000_000)
    );
    producer.send(data, null);
    _meterRegistry.counter(
        "ngrmdf_messages",
        "destinationTopic",
        data.topic()
    ).increment();
  }

  private byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(x);
    return buffer.array();
  }

  /**
   * Closes all producers.
   */
  public void close() {
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers =
        _producersRef.getAndSet(null);
    closeProducers(producers);
    _isRunning.set(false);
  }

  public boolean isRunning() {
    return _isRunning.get();
  }

  private KafkaProducer<DASMeasurementKey, DASMeasurement> producerForPartition(
      List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers,
      Integer partition) {
    if (producers.size() == 1) {
      return producers.get(0);
    }
    int p = partition == null ? 0 : partition;
    int idx = Math.floorMod(p, producers.size());
    return producers.get(idx);
  }

  private void closeProducers(
      List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers) {
    if (producers == null || producers.isEmpty()) {
      return;
    }
    for (KafkaProducer<DASMeasurementKey, DASMeasurement> producer : producers) {
      if (producer == null) {
        continue;
      }
      try {
        producer.flush();
      } catch (Exception e) {
        _logger.warn("Exception flushing producer: {}", e.getMessage());
      }
      try {
        producer.close(Duration.ofMillis(1000));
      } catch (Exception e) {
        _logger.warn("Exception closing producer: {}", e.getMessage());
      }
    }
  }
}
