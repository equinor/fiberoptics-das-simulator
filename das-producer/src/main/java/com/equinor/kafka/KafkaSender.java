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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

@Component
public class KafkaSender {

  private final AtomicReference<
      List<KafkaProducer<DASMeasurementKey, DASMeasurement>>> producersRef =
        new AtomicReference<>();
  private final MeterRegistry meterRegistry;

  public boolean isRunning = true;

  private static final Logger logger = LoggerFactory.getLogger(KafkaSender.class);

  public KafkaSender(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  public void setProducer(KafkaProducer<DASMeasurementKey, DASMeasurement> producer) {
    if (producer == null) {
      return;
    }
    setProducers(List.of(producer));
  }

  public void setProducers(List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers) {
    if (producers == null || producers.isEmpty()
        || producers.stream().anyMatch(Objects::isNull)) {
      return;
    }
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> previous =
        producersRef.getAndSet(List.copyOf(producers));
    closeProducers(previous);
    isRunning = true;
  }

  public void send(ProducerRecord<DASMeasurementKey, DASMeasurement> data) {
    if (!isRunning) {
      // logger.info("Producer not running");
      return;
    }
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers = producersRef.get();
    if (producers == null || producers.isEmpty()) {
      return;
    }
    KafkaProducer<DASMeasurementKey, DASMeasurement> producer =
        producerForPartition(producers, data.partition());

    Headers headers = data.headers();
    headers.add(
        KafkaHeaders.TIMESTAMP,
        longToBytes(data.value().getStartSnapshotTimeNano() / 1000000)
    );
    producer.send(data, null);
    meterRegistry.counter("ngrmdf_messages",
      "destinationTopic", data.topic()
    ).increment();
  }


  private byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(x);
    return buffer.array();
  }


  public void close() {
    List<KafkaProducer<DASMeasurementKey, DASMeasurement>> producers = producersRef.getAndSet(null);
    closeProducers(producers);
    isRunning = false;
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
        logger.warn("Exception flushing producer: {}", e.getMessage());
      }
      try {
        producer.close(Duration.ofMillis(1000));
      } catch (Exception e) {
        logger.warn("Exception closing producer: {}", e.getMessage());
      }
    }
  }
}
