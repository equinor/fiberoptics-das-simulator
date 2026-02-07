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

import static com.equinor.fiberoptics.das.producer.variants.util.Helpers.millisInNano;

import com.equinor.fiberoptics.das.error.ErrorCodeException;
import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Relays produced measurements to Kafka with partition ordering.
 */
@Component
public class KafkaRelay {

  private static final Logger _logger = LoggerFactory.getLogger(KafkaRelay.class);

  private final KafkaSender _kafkaSendChannel;
  private final KafkaConfiguration _kafkaConf;
  private final DasProducerConfiguration _dasProducerConfig;
  private final Map<Integer, ExecutorService> _partitionExecutors = new ConcurrentHashMap<>();
  private final AtomicBoolean _shuttingDown = new AtomicBoolean(false);

  KafkaRelay(
      KafkaConfiguration kafkaConfig,
      KafkaSender kafkaSendChannel,
      DasProducerConfiguration dasProducerConfiguration) {
    _kafkaConf = kafkaConfig;
    _kafkaSendChannel = kafkaSendChannel;
    _dasProducerConfig = dasProducerConfiguration;
  }

  /**
   * Enqueues a measurement for Kafka delivery.
   */
  public void relayToKafka(
      PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> partitionEntry
  ) {
    if (_shuttingDown.get()) {
      return;
    }
    Map<Integer, Integer> assignments = _dasProducerConfig.getPartitionAssignments();
    if (assignments == null || assignments.isEmpty()) {
      throw new ErrorCodeException(
          "KAFKA-001",
          HttpStatus.INTERNAL_SERVER_ERROR,
          "Partition assignments are missing. Cannot send measurement to Kafka."
      );
    }
    Integer currentPartition = assignments.get(partitionEntry.getValue().getLocus());
    if (currentPartition == null) {
      throw new ErrorCodeException(
          "KAFKA-002",
          HttpStatus.INTERNAL_SERVER_ERROR,
          "No partition assignment for locus "
              + partitionEntry.getValue().getLocus()
              + ". Available assignments: "
              + assignments.size()
      );
    }
    ProducerRecord<DASMeasurementKey, DASMeasurement> data = new ProducerRecord<>(
        _kafkaConf.getTopic(),
        currentPartition,
      partitionEntry.getValue().getStartSnapshotTimeNano() / millisInNano,
      partitionEntry.getKey(),
      partitionEntry.getValue()
    );
    try {
      ExecutorService executor = executorForPartition(currentPartition);
      long enqueueStartNanos = System.nanoTime();
      executor.execute(() -> {
        try {
          _kafkaSendChannel.send(data);
        } catch (InterruptException e) {
          if (!_shuttingDown.get()) {
            _logger.warn(
                "Interrupted while sending to partition {}: {}",
                currentPartition,
                e.getMessage()
            );
          }
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          if (!_shuttingDown.get()) {
            _logger.warn(
                "Failed to send to partition {}: {}",
                currentPartition,
                e.getMessage()
            );
          }
        }
      });
      maybeWarnOnBlockedEnqueue(currentPartition, executor, enqueueStartNanos);
    } catch (RejectedExecutionException e) {
      if (!_shuttingDown.get()) {
        _logger.warn(
            "Send rejected for partition {}: {}",
            currentPartition,
            e.getMessage()
        );
      }
    }
    _logger.debug(
        "Now: {}, sent fiber shot with content nano-timestamp: {}, and index timestamp: {}",
        System.currentTimeMillis(),
        data.value().getStartSnapshotTimeNano(),
        data.timestamp()
    );
  }

  /**
   * Shuts down partition executors and closes the sender.
   */
  public void teardown() {
    _logger.info("Send complete. Shutting down thread now");
    _shuttingDown.set(true);
    for (ExecutorService executor : _partitionExecutors.values()) {
      executor.shutdown();
    }
    for (ExecutorService executor : _partitionExecutors.values()) {
      try {
        long timeoutMillis = 10_000L;
        if (_kafkaConf.getRelayShutdownTimeout() != null) {
          timeoutMillis = Math.max(1L, _kafkaConf.getRelayShutdownTimeout().toMillis());
        }
        if (!executor.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    _partitionExecutors.clear();
    _kafkaSendChannel.close();
    _shuttingDown.set(false);
  }

  private ExecutorService executorForPartition(int partition) {
    return _partitionExecutors.computeIfAbsent(partition, this::createPartitionExecutor);
  }

  private ExecutorService createPartitionExecutor(Integer partition) {
    int queueCapacity = Math.max(1, _kafkaConf.getRelayQueueCapacity());
    long enqueueTimeoutMillis = _kafkaConf.getRelayEnqueueTimeoutMillis();
    BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueCapacity);

    ThreadFactory threadFactory = r -> {
      Thread thread = new Thread(r, "kafka-partition-sender-" + partition);
      thread.setDaemon(true);
      return thread;
    };

    RejectedExecutionHandler rejectionHandler = new BlockingEnqueuePolicy(
        _shuttingDown,
        enqueueTimeoutMillis
    );
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

  private void maybeWarnOnBlockedEnqueue(
      int partition,
      ExecutorService executor,
      long enqueueStartNanos) {
    long warnMillis = _kafkaConf.getRelayEnqueueWarnMillis();
    if (warnMillis <= 0) {
      return;
    }
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
        System.nanoTime() - enqueueStartNanos
    );
    if (elapsedMillis < warnMillis) {
      return;
    }
    if (executor instanceof ThreadPoolExecutor tpe) {
      int queueSize = tpe.getQueue().size();
      int queueCapacity = queueSize + tpe.getQueue().remainingCapacity();
      _logger.warn(
          "Backpressure: blocked {}ms enqueueing send to partition {} "
            + "(queue {}/{}, activeThreads={})",
          elapsedMillis,
          partition,
          queueSize,
          queueCapacity,
          tpe.getActiveCount()
      );
    } else {
      _logger.warn(
          "Backpressure: blocked {}ms enqueueing send to partition {}",
          elapsedMillis,
          partition
      );
    }
  }

  private static final class BlockingEnqueuePolicy implements RejectedExecutionHandler {
    private final AtomicBoolean _shuttingDown;
    private final long _enqueueTimeoutMillis;

    private BlockingEnqueuePolicy(AtomicBoolean shuttingDown, long enqueueTimeoutMillis) {
      _shuttingDown = shuttingDown;
      _enqueueTimeoutMillis = enqueueTimeoutMillis;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      if (executor.isShutdown() || _shuttingDown.get()) {
        throw new RejectedExecutionException("Executor is shut down");
      }
      try {
        if (_enqueueTimeoutMillis <= 0) {
          executor.getQueue().put(r);
          return;
        }
        boolean accepted = executor.getQueue().offer(
            r,
            _enqueueTimeoutMillis,
            TimeUnit.MILLISECONDS
        );
        if (!accepted) {
          throw new RejectedExecutionException(
              "Timed out waiting for queue space (" + _enqueueTimeoutMillis + "ms)"
          );
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException("Interrupted while waiting for queue space", e);
      }
    }
  }

}
