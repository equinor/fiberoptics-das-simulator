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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class KafkaSender {

  private final AtomicReference<KafkaProducer<DASMeasurementKey, DASMeasurement>> producerRef = new AtomicReference<>();
  private final MeterRegistry meterRegistry;

  public boolean isRunning = true;

  private static final Logger logger = LoggerFactory.getLogger(KafkaSender.class);

  public KafkaSender(MeterRegistry meterRegistry, ObjectProvider<KafkaProducer<DASMeasurementKey, DASMeasurement>> producerProvider) {
    this.meterRegistry = meterRegistry;
    KafkaProducer<DASMeasurementKey, DASMeasurement> producer = producerProvider.getIfAvailable();
    if (producer != null) {
      producerRef.set(producer);
    }
  }

  public void setProducer(KafkaProducer<DASMeasurementKey, DASMeasurement> producer) {
    if (producer == null) {
      return;
    }
    producerRef.set(producer);
    isRunning = true;
  }

  public void send(ProducerRecord<DASMeasurementKey, DASMeasurement> data) {
    if (!isRunning) {
      // logger.info("Producer not running");
      return;
    }
    KafkaProducer<DASMeasurementKey, DASMeasurement> producer = producerRef.get();
    if (producer == null) {
      return;
    }

    Headers headers = data.headers();
    headers.add(KafkaHeaders.TIMESTAMP, longToBytes(data.value().getStartSnapshotTimeNano() / 1000000));
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
    KafkaProducer<DASMeasurementKey, DASMeasurement> producer = producerRef.getAndSet(null);
    try {
      if (producer != null) {
        producer.flush();
      }
    } catch (Exception e) {
      logger.warn("Exception flushing producer: {}", e.getMessage());
    }
    try {
      if (producer != null) {
        producer.close(Duration.ofMillis(1000));
      }
    } catch (Exception e) {
      logger.warn("Exception closing producer: {}", e.getMessage());
    }
    isRunning = false;
  }
}
