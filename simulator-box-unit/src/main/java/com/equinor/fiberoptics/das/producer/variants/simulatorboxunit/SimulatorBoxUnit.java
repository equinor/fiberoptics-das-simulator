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

import com.equinor.fiberoptics.das.producer.variants.GenericDasProducer;
import com.equinor.fiberoptics.das.producer.variants.PackageStepCalculator;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import com.equinor.fiberoptics.das.producer.variants.util.Helpers;
import com.equinor.fiberoptics.das.producer.variants.util.TimingStats;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This is an example DAS  box unit implementation.
 * It's role is to convert the raw DAS data into a format that can be accepted into the Kafka server environment.
 * Serving the amplitude data
 *
 * @author Espen Tjonneland, espen@tjonneland.no
 */
@Component("SimulatorBoxUnit")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@EnableConfigurationProperties({ SimulatorBoxUnitConfiguration.class})
public class SimulatorBoxUnit implements GenericDasProducer {
  private static final Logger logger = LoggerFactory.getLogger(SimulatorBoxUnit.class);

  private final SimulatorBoxUnitConfiguration _configuration;
  private final PackageStepCalculator _stepCalculator;

  public SimulatorBoxUnit(SimulatorBoxUnitConfiguration configuration)
  {
    this._configuration = configuration;
    this._stepCalculator = new PackageStepCalculator(_configuration.getStartTimeInstant(),
      _configuration.getMaxFreq(), _configuration.getAmplitudesPrPackage(), _configuration.getNumberOfLoci());
  }

  @Override
  public Flux<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> produce() {
    RandomDataCache dataCache = new RandomDataCache(_configuration.getNumberOfPrePopulatedValues(), _configuration.getAmplitudesPrPackage(), _configuration.getPulseRate(), _configuration.getAmplitudeDataType());
    Duration delay = _configuration.isDisableThrottling() ? Duration.ZERO : Duration.ofNanos(_stepCalculator.nanosPrPackage());
    long take;
    if (_configuration.getNumberOfShots() != null && _configuration.getNumberOfShots() > 0) {
      take = _configuration.getNumberOfShots().intValue();
      logger.info(String.format("Starting to produce %d data", take));

    } else {
      take = delay.isZero() ? _configuration.getSecondsToRun() * 1000 : (long) (_configuration.getSecondsToRun() / (delay.toNanos() / 1_000_000_000.0));
      logger.info(String.format("Starting to produce data now for %d seconds", _configuration.getSecondsToRun()));

    }
    final long takeFinal = take;
    return Flux.defer(() -> {
      TimingStats timingStats = new TimingStats();
      ScheduledExecutorService timingLogger = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "simulatorbox-timing-logger");
        thread.setDaemon(true);
        return thread;
      });
      timingLogger.scheduleAtFixedRate(() -> logTimingSummary(timingStats), 30, 30, TimeUnit.SECONDS);
      return Flux.create(sink -> {
        Thread producerThread = new Thread(() -> {
          long emitted = 0;
          try {
            while (!sink.isCancelled() && emitted < takeFinal) {
              long nowNanos = Helpers.currentEpochNanos();
              if (paceAndMaybeDrop(nowNanos, timingStats)) {
                sink.next(Collections.<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>emptyList());
                emitted++;
                continue;
              }
              List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>> data = IntStream.range(0, _configuration.getNumberOfLoci())
                .mapToObj(currentLocus -> constructAvroObjects(currentLocus, dataCache.getFloat(), dataCache.getLong()))
                .collect(Collectors.toList());
              _stepCalculator.increment(1);
              sink.next(data);
              emitted++;
            }
            sink.complete();
          } catch (Throwable ex) {
            sink.error(ex);
          } finally {
            logTimingSummary(timingStats);
            timingLogger.shutdownNow();
          }
        }, "simulatorbox-producer");
        producerThread.setDaemon(true);
        producerThread.start();
      });
    });
  }

  private boolean paceAndMaybeDrop(long wallClockEpochNanos, TimingStats timingStats) {
    long targetEpochNanos = _stepCalculator.currentEpochNanos();
    long deltaNanos = targetEpochNanos - wallClockEpochNanos;
    long warnNanos = _configuration.getTimeLagWarnMillis() * Helpers.millisInNano;
    boolean warnLag = deltaNanos < 0 && warnNanos > 0 && (-deltaNanos) > warnNanos;
    boolean pacingEnabled = _configuration.isTimePacingEnabled();
    if (deltaNanos > 0) {
      long sleptNanos = 0;
      long preSleepLeadNanos = deltaNanos;
      if (pacingEnabled) {
        Helpers.sleepNanos(deltaNanos);
        sleptNanos = deltaNanos;
      }
      long postDeltaNanos = pacingEnabled ? targetEpochNanos - Helpers.currentEpochNanos() : deltaNanos;
      timingStats.record(postDeltaNanos, sleptNanos, false, false, preSleepLeadNanos);
      return false;
    }
    if (!pacingEnabled) {
      timingStats.record(deltaNanos, 0, false, warnLag);
      return false;
    }
    long lagNanos = -deltaNanos;
    long dropNanos = _configuration.getTimeLagDropMillis() * Helpers.millisInNano;
    if (dropNanos > 0 && lagNanos > dropNanos) {
      logger.warn("Dropping package to catch up. Lag={} ms (target={}, now={})",
        lagNanos / Helpers.millisInNano, targetEpochNanos, wallClockEpochNanos);
      timingStats.record(deltaNanos, 0, true, warnLag);
      _stepCalculator.increment(1);
      return true;
    }
    timingStats.record(deltaNanos, 0, false, warnLag);
    return false;
  }

  private void logTimingSummary(TimingStats timingStats) {
    TimingStats.Summary summary = timingStats.snapshotAndReset();
    if (summary.totalPackages == 0) {
      return;
    }
    double avgLagMs = summary.lagCount == 0 ? 0 : (summary.totalLagNanos / 1_000_000.0) / summary.lagCount;
    double avgLeadMs = summary.leadCount == 0 ? 0 : (summary.totalLeadNanos / 1_000_000.0) / summary.leadCount;
    double avgSleepMs = summary.sleepCount == 0 ? 0 : (summary.totalSleepNanos / 1_000_000.0) / summary.sleepCount;
    double avgPreSleepLeadMs = summary.preSleepLeadCount == 0 ? 0 : (summary.totalPreSleepLeadNanos / 1_000_000.0) / summary.preSleepLeadCount;
    double avgDeltaMs = summary.totalPackages == 0 ? 0 : (summary.totalDeltaNanos / 1_000_000.0) / summary.totalPackages;
    double lastDeltaMs = summary.lastDeltaNanos / 1_000_000.0;
    String avgDeltaDir = avgDeltaMs < 0 ? "behind" : "ahead";
    String lastDeltaDir = lastDeltaMs < 0 ? "behind" : "ahead";
    String message = new StringBuilder(256)
      .append("Timing summary (last 30s)\n")
      .append("  Fiber shots: ").append(summary.totalPackages)
      .append(" (drops: ").append(summary.dropCount).append(")\n")
      .append("  Delta vs wall clock: avg ")
      .append(String.format("%.1f", Math.abs(avgDeltaMs))).append(" ms ").append(avgDeltaDir)
      .append(", last ").append(String.format("%.1f", Math.abs(lastDeltaMs))).append(" ms ").append(lastDeltaDir).append("\n")
      .append("  Lagging fiber shots: ").append(summary.lagCount)
      .append(" (avg ").append(String.format("%.1f", avgLagMs)).append(" ms, max ")
      .append(summary.maxLagNanos / Helpers.millisInNano).append(" ms)\n")
      .append("  Leading fiber shots: ").append(summary.leadCount)
      .append(" (avg ").append(String.format("%.1f", avgLeadMs)).append(" ms, max ")
      .append(summary.maxLeadNanos / Helpers.millisInNano).append(" ms)\n")
      .append("  Pacing: ").append(_configuration.isTimePacingEnabled())
      .append(" (sleeps ").append(summary.sleepCount).append(", avg ")
      .append(String.format("%.1f", avgSleepMs)).append(" ms")
      .append(", pre-sleep lead avg ").append(String.format("%.1f", avgPreSleepLeadMs)).append(" ms)\n")
      .append("  Package duration: ").append(String.format("%.1f", _stepCalculator.nanosPrPackage() / 1_000_000.0)).append(" ms")
      .toString();
    logger.info(message);
  }

  private PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> constructAvroObjects(int currentLocus, List<Float> floatData, List<Long> longData) {
    return new PartitionKeyValueEntry<>(
      DASMeasurementKey.newBuilder()
        .setLocus(currentLocus)
        .build(),
      DASMeasurement.newBuilder()
        .setStartSnapshotTimeNano(_stepCalculator.currentEpochNanos())
        .setTrustedTimeSource(true)
        .setLocus(currentLocus)
        .setAmplitudesFloat(floatData)
        .setAmplitudesLong(longData)
        .build(),
      currentLocus);
  }
}
