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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.Duration;

@Component
public class KafkaSender {

  @Autowired
  private KafkaProducer<DASMeasurementKey, DASMeasurement> producer;

  @Autowired
  private MeterRegistry meterRegistry;

  public boolean isRunning = true;

  private static final Logger logger = LoggerFactory.getLogger(KafkaSender.class);

  private static final long msBetweenPerformanceOutput = 10000;
  private long iterationStartTime = System.currentTimeMillis();
  private long messageCount = 0L;

  public void send(ProducerRecord<DASMeasurementKey, DASMeasurement> data) {
    if (!isRunning) {
      // logger.info("Producer not running");
      return;
    }

    Headers headers = data.headers();
    headers.add(KafkaHeaders.TIMESTAMP, longToBytes(data.value().getStartSnapshotTimeNano() / 1000000));
    producer.send(data, null);
    messageCount++;

    meterRegistry.counter("ngrmdf_messages",
      "destinationTopic", data.topic()
    ).increment();

    if (System.currentTimeMillis() - iterationStartTime >= msBetweenPerformanceOutput) {
      if (messageCount > 0) {
        logger.info("We are producing {} messages pr second.", (messageCount / (msBetweenPerformanceOutput / 1000)));
      }
      messageCount = 0L;
      iterationStartTime = System.currentTimeMillis();
    }
  }


  private byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(x);
    return buffer.array();
  }


  public void close() {
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
    isRunning = false;
  }
}
