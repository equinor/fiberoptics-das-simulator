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
package com.equinor.fiberoptics.das.producer.variants.simulatorboxunit;

import com.equinor.fiberoptics.das.PackageStepCalculator;
import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.GenericDasProducer;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import com.equinor.kafka.KafkaConfiguration;
import com.equinor.kafka.KafkaSender;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/**
 * This is an example DAS  box unit implementation.
 * It's role is to convert the raw DAS data into a format that can be accepted into the Kafka server environment.
 * Serving the amplitude data
 *
 * @author Espen Tjonneland, espen@tjonneland.no
 */
@Component("SimulatorBoxUnit")
public class SimulatorBoxUnit implements GenericDasProducer {

  private static final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
  private final KafkaSender _kafkaSendChannel;
  private final RandomDataCache _randomDataCache;
  private final KafkaConfiguration _kafkaConf;
  private final SimulatorBoxUnitConfiguration _simConfig;
  private final DasProducerConfiguration _dasProducerConfig;

  private PackageStepCalculator stepCalculator;
  private boolean done;

  private static final Logger logger = LoggerFactory.getLogger(SimulatorBoxUnit.class);

  SimulatorBoxUnit(KafkaConfiguration kafkaConfig, KafkaSender kafkaSendChannel, SimulatorBoxUnitConfiguration simConfig,
                   DasProducerConfiguration dasProducerConfiguration) {
    _kafkaConf = kafkaConfig;
    _kafkaSendChannel = kafkaSendChannel;
    _simConfig = simConfig;
    _dasProducerConfig = dasProducerConfiguration;
    _randomDataCache = new RandomDataCache(100, _dasProducerConfig.getAmplitudesPrPackage(), _simConfig.getPulseRate());

    logger.info(String.format("Starting to produce data now for %d seconds", _simConfig.getSecondsToRun()));

  }

  public boolean isDone() {
    return done;
  }

  public void startDataStreaming() {
    scheduler.setPoolSize(1);
    scheduler.initialize();

    stepCalculator = new PackageStepCalculator(Instant.now(),
      _simConfig.getMaxFreq(), _dasProducerConfig.getAmplitudesPrPackage(), _simConfig.getNumberOfLoci());

    long runtimeExpectation = _simConfig.getSecondsToRun() * 1000;

    long millisIntervalBetweenShots = (long) stepCalculator.millisPrPackage();
    PeriodicTrigger trigger;
    if (_simConfig.isDisableThrotteling()) {
      logger.info("Will not attempt to throttle send rate.");
      trigger = new PeriodicTrigger(0, TimeUnit.MILLISECONDS);
      trigger.setFixedRate(false);
    } else {
      double sendOpsPrSecond = 1 / stepCalculator.secondsPrPackage();
      logger.info("The send rate should be {} * total number of loci: {} , meaning apprx {} messages pr second.",
        sendOpsPrSecond, _simConfig.getNumberOfLoci(), (long) (sendOpsPrSecond * _simConfig.getNumberOfLoci()));
      trigger = new PeriodicTrigger(millisIntervalBetweenShots, TimeUnit.MILLISECONDS);
      trigger.setFixedRate(true);
    }

    ScheduledFuture<?> schedule = scheduler.schedule(getTask(), trigger);
    _dasProducerConfig.signalRunning();

    try {
      logger.info("Application is configured to run for {} minutes before stopping.",
        Duration.ofMillis(runtimeExpectation).toMinutes());
      Thread.sleep(runtimeExpectation);
    } catch (InterruptedException e) {
      logger.error(e.getMessage());
    }

    logger.info("Stopping");
    _dasProducerConfig.signalStopped();

    do {
      try {
        logger.info("Waiting for send to complete");
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        logger.error(e.getMessage());
      }
    } while (_dasProducerConfig.isSending());

    logger.info("Send complete. Shutting down thread now");
    _kafkaSendChannel.close();
    if (schedule != null) {
      schedule.cancel(false);
    }
    this.done = true;
  }

  private Runnable getTask() {
    return () -> {
      if (_dasProducerConfig.isRunning()) {

        //This loop creates an entire fiber length shot with the same time for all loci.
        for (int currentLocus = 0; currentLocus < _simConfig.getNumberOfLoci(); currentLocus++) {
          PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> partitionEntry =
            constructAvroObjects(currentLocus, _randomDataCache.getFloat());
          int currentPartition = _dasProducerConfig.getPartitionAssignments().get(partitionEntry.value.getLocus()); //Use the one from stream initiator (Simulator mode)

          logger.debug("Sending amplitude array for locus: {} to partition: {} total partitions: {}",
            partitionEntry.value.getLocus(), currentPartition, _kafkaConf.getPartitions());
          ProducerRecord<DASMeasurementKey, DASMeasurement> data =
            new ProducerRecord(_kafkaConf.getTopic(),
              currentPartition,
              stepCalculator.currentEpochMillis(),
              partitionEntry.key, partitionEntry.value);

          _dasProducerConfig.signalSending();
          _kafkaSendChannel.send(data);
          _dasProducerConfig.signalNotSending();
        }
        stepCalculator.increment(1);
      } else {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          logger.warn("Sleep int: {}", e.getMessage());
        }
      }
    };
  }

  private PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> constructAvroObjects(int currentLocus, List<Float> data) {
    return new PartitionKeyValueEntry<>(
      DASMeasurementKey.newBuilder()
        .setLocus(currentLocus)
        .build(),
      DASMeasurement.newBuilder()
        .setStartSnapshotTimeNano(stepCalculator.currentEpochNanos())
        .setTrustedTimeSource(true)
        .setLocus(currentLocus)
        .setAmplitudesFloat(data)
        .build(),
      currentLocus);
  }
}
