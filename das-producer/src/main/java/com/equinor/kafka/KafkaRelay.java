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
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.equinor.fiberoptics.das.producer.variants.util.Helpers.millisInNano;

@Component
public class KafkaRelay {

  private static final Logger logger = LoggerFactory.getLogger(KafkaRelay.class);

  private final KafkaSender _kafkaSendChannel;
  private final KafkaConfiguration _kafkaConf;
  private final DasProducerConfiguration _dasProducerConfig;
  private final Map<Integer, ExecutorService> partitionExecutors = new ConcurrentHashMap<>();
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

  KafkaRelay(
    KafkaConfiguration kafkaConfig,
    KafkaSender kafkaSendChannel,
    DasProducerConfiguration dasProducerConfiguration) {
    this._kafkaConf = kafkaConfig;
    this._kafkaSendChannel = kafkaSendChannel;
    this._dasProducerConfig = dasProducerConfiguration;
  }

  public void relayToKafka(PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> partitionEntry) {
    if (shuttingDown.get()) {
      return;
    }
    int currentPartition = _dasProducerConfig.getPartitionAssignments().get(partitionEntry.value.getLocus()); //Use the one from stream initiator (Simulator mode)
    ProducerRecord<DASMeasurementKey, DASMeasurement> data =
      new ProducerRecord(_kafkaConf.getTopic(),
        currentPartition,
        partitionEntry.value.getStartSnapshotTimeNano() / millisInNano,
        partitionEntry.key, partitionEntry.value);
    try {
      executorForPartition(currentPartition).execute(() -> {
        try {
          _kafkaSendChannel.send(data);
        } catch (InterruptException e) {
          if (!shuttingDown.get()) {
            logger.warn("Interrupted while sending to partition {}: {}", currentPartition, e.getMessage());
          }
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          if (!shuttingDown.get()) {
            logger.warn("Failed to send to partition {}: {}", currentPartition, e.getMessage());
          }
        }
      });
    } catch (RejectedExecutionException e) {
      if (!shuttingDown.get()) {
        logger.warn("Send rejected for partition {}: {}", currentPartition, e.getMessage());
      }
    }
    logger.debug("Now: {}, sent fiber shot with content nano-timestamp: {}, and index timestamp: {}",
      System.currentTimeMillis(), data.value().getStartSnapshotTimeNano(), data.timestamp());
  }

  public void teardown() {
    logger.info("Send complete. Shutting down thread now");
    shuttingDown.set(true);
    for (ExecutorService executor : partitionExecutors.values()) {
      executor.shutdown();
    }
    for (ExecutorService executor : partitionExecutors.values()) {
      try {
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    partitionExecutors.clear();
    _kafkaSendChannel.close();
    shuttingDown.set(false);
  }

  private ExecutorService executorForPartition(int partition) {
    return partitionExecutors.computeIfAbsent(partition, this::createPartitionExecutor);
  }

  private ExecutorService createPartitionExecutor(int partition) {
    int queueCapacity = Math.max(1, _kafkaConf.getRelayQueueCapacity());
    long enqueueTimeoutMillis = _kafkaConf.getRelayEnqueueTimeoutMillis();
    BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueCapacity);

    ThreadFactory threadFactory = r -> {
      Thread thread = new Thread(r, "kafka-partition-sender-" + partition);
      thread.setDaemon(true);
      return thread;
    };

    RejectedExecutionHandler rejectionHandler = new BlockingEnqueuePolicy(shuttingDown, enqueueTimeoutMillis);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      queue,
      threadFactory,
      rejectionHandler
    );
    return executor;
  }

  private static final class BlockingEnqueuePolicy implements RejectedExecutionHandler {
    private final AtomicBoolean shuttingDown;
    private final long enqueueTimeoutMillis;

    private BlockingEnqueuePolicy(AtomicBoolean shuttingDown, long enqueueTimeoutMillis) {
      this.shuttingDown = shuttingDown;
      this.enqueueTimeoutMillis = enqueueTimeoutMillis;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown() || shuttingDown.get()) {
        throw new RejectedExecutionException("Executor is shut down");
      }
      try {
        if (enqueueTimeoutMillis <= 0) {
          executor.getQueue().put(r);
          return;
        }
        boolean accepted = executor.getQueue().offer(r, enqueueTimeoutMillis, TimeUnit.MILLISECONDS);
        if (!accepted) {
          throw new RejectedExecutionException("Timed out waiting for queue space (" + enqueueTimeoutMillis + "ms)");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException("Interrupted while waiting for queue space", e);
      }
    }
  }

}
