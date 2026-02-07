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
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * This is an example DAS  box unit implementation.
 * It's role is to convert the raw DAS data into a format that can be accepted into the Kafka server
 * environment.
 * Serving the amplitude data
 *
 * @author Espen Tjonneland, espen@tjonneland.no
 */
@Component("SimulatorBoxUnit")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@EnableConfigurationProperties({SimulatorBoxUnitConfiguration.class})
public class SimulatorBoxUnit implements GenericDasProducer {
  private static final Logger _logger = LoggerFactory.getLogger(SimulatorBoxUnit.class);
  private static final Duration _TIMING_LOG_INTERVAL = Duration.ofSeconds(30);

  private final SimulatorBoxUnitConfiguration _configuration;
  private final PackageStepCalculator _stepCalculator;

  /**
   * Creates a new simulator box unit.
   */
  public SimulatorBoxUnit(SimulatorBoxUnitConfiguration configuration) {
    _configuration = copyConfiguration(configuration);
    _stepCalculator = new PackageStepCalculator(
        _configuration.getStartTimeInstant(),
        _configuration.getMaxFreq(),
        _configuration.getAmplitudesPrPackage(),
        _configuration.getNumberOfLoci()
    );
  }

  /**
   * Produces simulated measurements.
   */
  @Override
  public Flux<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> produce() {
    RandomDataCache dataCache = new RandomDataCache(
        _configuration.getNumberOfPrePopulatedValues(),
        _configuration.getAmplitudesPrPackage(),
        _configuration.getPulseRate(),
        _configuration.getAmplitudeDataType()
    );
    Duration delay = _configuration.isDisableThrottling()
        ? Duration.ZERO
        : Duration.ofNanos(_stepCalculator.nanosPrPackage());
    if (_configuration.isDisableThrottling() && _configuration.isTimePacingEnabled()) {
      _logger.info("disableThrottling=true: pacing and drop-to-catch-up are disabled.");
    }
    long take;
    if (_configuration.getNumberOfShots() != null && _configuration.getNumberOfShots() > 0) {
      take = _configuration.getNumberOfShots().intValue();
      _logger.info("Starting to produce {} data", take);
    } else {
      double packagesPerSecond = _configuration.getSecondsToRun()
          / (delay.toNanos() / 1_000_000_000.0);
      take = delay.isZero()
          ? _configuration.getSecondsToRun() * 1000L
          : (long) packagesPerSecond;
      _logger.info(
          "Starting to produce data now for {} seconds",
          _configuration.getSecondsToRun()
      );
    }
    final long takeFinal = take;
    return Flux.defer(() -> {
      TimingStats timingStats = new TimingStats();
      Scheduler timingScheduler = Schedulers.newSingle("simulatorbox-timing-logger");
      Disposable timingDisposable = Flux.interval(_TIMING_LOG_INTERVAL, timingScheduler)
          .doOnNext(tick -> logTimingSummary(timingStats))
          .subscribe();
      Scheduler producerScheduler = Schedulers.newSingle("simulatorbox-producer");
      AtomicBoolean cleanedUp = new AtomicBoolean(false);
      return Flux.create(sink -> {
        Scheduler.Worker worker = producerScheduler.createWorker();
        AtomicLong emitted = new AtomicLong(0);
        AtomicBoolean startTimeAligned = new AtomicBoolean(false);
        Runnable[] loop = new Runnable[1];
        loop[0] = () -> {
          if (sink.isCancelled()) {
            cleanup(
                timingStats,
                timingDisposable,
                timingScheduler,
                producerScheduler,
                worker,
                cleanedUp
            );
            return;
          }
          if (emitted.get() >= takeFinal) {
            sink.complete();
            cleanup(
                timingStats,
                timingDisposable,
                timingScheduler,
                producerScheduler,
                worker,
                cleanedUp
            );
            return;
          }
          long nowNanos = Helpers.currentEpochNanos();
          if (!startTimeAligned.get() && _configuration.getStartTimeEpochSecond() == 0) {
            _stepCalculator.resetCurrentEpochNanos(nowNanos);
            startTimeAligned.set(true);
          }
          PaceDecision decision = evaluatePacing(nowNanos);
          if (decision._delayNanos > 0) {
            worker.schedule(
                () -> emitAfterDelay(
                    decision,
                    emitted,
                    takeFinal,
                    sink,
                    timingStats,
                    dataCache,
                    loop[0],
                    timingDisposable,
                    timingScheduler,
                    producerScheduler,
                    worker,
                    cleanedUp
                ),
                decision._delayNanos,
                TimeUnit.NANOSECONDS
            );
            return;
          }
          emitNow(
              decision,
              emitted,
              takeFinal,
              sink,
              timingStats,
              dataCache,
              loop[0],
              timingDisposable,
              timingScheduler,
              producerScheduler,
              worker,
              cleanedUp
          );
        };
        worker.schedule(loop[0]);
      });
    });
  }

  private PaceDecision evaluatePacing(long wallClockEpochNanos) {
    long targetEpochNanos = _stepCalculator.currentEpochNanos();
    long deltaNanos = targetEpochNanos - wallClockEpochNanos;
    long warnNanos = _configuration.getTimeLagWarnMillis() * Helpers.millisInNano;
    boolean warnLag = deltaNanos < 0
        && warnNanos > 0
        && (-deltaNanos) > warnNanos;
    boolean pacingEnabled = _configuration.isTimePacingEnabled()
        && !_configuration.isDisableThrottling();
    if (deltaNanos > 0) {
      return pacingEnabled
          ? PaceDecision.delay(deltaNanos, targetEpochNanos)
          : PaceDecision.immediate(deltaNanos, warnLag);
    }
    long lagNanos = -deltaNanos;
    long dropNanos = _configuration.getTimeLagDropMillis() * Helpers.millisInNano;
    boolean syntheticStartTimeConfigured = _configuration.getStartTimeEpochSecond() != 0;
    if (pacingEnabled
        && !syntheticStartTimeConfigured
        && dropNanos > 0
        && lagNanos > dropNanos) {
      _logger.warn(
          "Dropping package to catch up. Lag={} ms (target={}, now={})",
          lagNanos / Helpers.millisInNano,
          targetEpochNanos,
          wallClockEpochNanos
      );
      return PaceDecision.drop(deltaNanos, warnLag);
    }
    return PaceDecision.immediate(deltaNanos, warnLag);
  }

  private void emitAfterDelay(
      PaceDecision decision,
      AtomicLong emitted,
      long takeFinal,
      FluxSink<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> sink,
      TimingStats timingStats,
      RandomDataCache dataCache,
      Runnable loop,
      Disposable timingDisposable,
      Scheduler timingScheduler,
      Scheduler producerScheduler,
      Scheduler.Worker worker,
      AtomicBoolean cleanedUp
  ) {
    if (sink.isCancelled()) {
      cleanup(
          timingStats,
          timingDisposable,
          timingScheduler,
          producerScheduler,
          worker,
          cleanedUp
      );
      return;
    }
    if (emitted.get() >= takeFinal) {
      sink.complete();
      cleanup(
          timingStats,
          timingDisposable,
          timingScheduler,
          producerScheduler,
          worker,
          cleanedUp
      );
      return;
    }
    long nowNanos = Helpers.currentEpochNanos();
    long postDeltaNanos = decision._targetEpochNanos - nowNanos;
    timingStats.record(
        postDeltaNanos,
        decision._delayNanos,
        false,
        false,
        decision._delayNanos
    );
    emitData(sink, dataCache);
    _stepCalculator.increment(1);
    emitted.incrementAndGet();
    worker.schedule(loop);
  }

  private void emitNow(
      PaceDecision decision,
      AtomicLong emitted,
      long takeFinal,
      FluxSink<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> sink,
      TimingStats timingStats,
      RandomDataCache dataCache,
      Runnable loop,
      Disposable timingDisposable,
      Scheduler timingScheduler,
      Scheduler producerScheduler,
      Scheduler.Worker worker,
      AtomicBoolean cleanedUp
  ) {
    if (decision._drop) {
      timingStats.record(decision._deltaNanos, 0, true, decision._warnLag);
      _stepCalculator.increment(1);
      sink.next(
          Collections.<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>emptyList()
      );
      emitted.incrementAndGet();
      worker.schedule(loop);
      return;
    }
    timingStats.record(decision._deltaNanos, 0, false, decision._warnLag);
    emitData(sink, dataCache);
    _stepCalculator.increment(1);
    emitted.incrementAndGet();
    if (emitted.get() >= takeFinal) {
      sink.complete();
      cleanup(
          timingStats,
          timingDisposable,
          timingScheduler,
          producerScheduler,
          worker,
          cleanedUp
      );
      return;
    }
    worker.schedule(loop);
  }

  private void emitData(
      FluxSink<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> sink,
      RandomDataCache dataCache
  ) {
    List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>> data =
        IntStream.range(0, _configuration.getNumberOfLoci())
            .mapToObj(
                currentLocus -> constructAvroObjects(
                    currentLocus,
                    dataCache.getFloat(),
                    dataCache.getLong()
                )
            )
            .collect(Collectors.toList());
    sink.next(data);
  }

  private void cleanup(
      TimingStats timingStats,
      Disposable timingDisposable,
      Scheduler timingScheduler,
      Scheduler producerScheduler,
      Scheduler.Worker worker,
      AtomicBoolean cleanedUp
  ) {
    if (!cleanedUp.compareAndSet(false, true)) {
      return;
    }
    logTimingSummary(timingStats);
    timingDisposable.dispose();
    timingScheduler.dispose();
    worker.dispose();
    producerScheduler.dispose();
  }

  private static class PaceDecision {
    private final long _deltaNanos;
    private final boolean _warnLag;
    private final boolean _drop;
    private final long _delayNanos;
    private final long _targetEpochNanos;

    private PaceDecision(
        long deltaNanos,
        boolean warnLag,
        boolean drop,
        long delayNanos,
        long targetEpochNanos
    ) {
      _deltaNanos = deltaNanos;
      _warnLag = warnLag;
      _drop = drop;
      _delayNanos = delayNanos;
      _targetEpochNanos = targetEpochNanos;
    }

    private static PaceDecision delay(long delayNanos, long targetEpochNanos) {
      return new PaceDecision(
          delayNanos,
          false,
          false,
          delayNanos,
          targetEpochNanos
      );
    }

    private static PaceDecision immediate(long deltaNanos, boolean warnLag) {
      return new PaceDecision(deltaNanos, warnLag, false, 0, 0);
    }

    private static PaceDecision drop(long deltaNanos, boolean warnLag) {
      return new PaceDecision(deltaNanos, warnLag, true, 0, 0);
    }
  }

  private void logTimingSummary(TimingStats timingStats) {
    TimingStats.Summary summary = timingStats.snapshotAndReset();
    if (summary.getTotalPackages() == 0) {
      return;
    }
    double avgLagMs = summary.getLagCount() == 0
        ? 0
      : (summary.getTotalLagNanos() / 1_000_000.0) / summary.getLagCount();
    double avgLeadMs = summary.getLeadCount() == 0
        ? 0
      : (summary.getTotalLeadNanos() / 1_000_000.0) / summary.getLeadCount();
    double avgSleepMs = summary.getSleepCount() == 0
        ? 0
      : (summary.getTotalSleepNanos() / 1_000_000.0) / summary.getSleepCount();
    double avgPreSleepLeadMs = summary.getPreSleepLeadCount() == 0
        ? 0
      : (summary.getTotalPreSleepLeadNanos() / 1_000_000.0)
        / summary.getPreSleepLeadCount();
    double avgDeltaMs = summary.getTotalPackages() == 0
        ? 0
      : (summary.getTotalDeltaNanos() / 1_000_000.0) / summary.getTotalPackages();
    double lastDeltaMs = summary.getLastDeltaNanos() / 1_000_000.0;
    long intervalSeconds = Math.max(1, _TIMING_LOG_INTERVAL.getSeconds());
    double packagesPerSecond = summary.getTotalPackages() / (double) intervalSeconds;
    String avgDeltaDir = avgDeltaMs < 0 ? "behind" : "ahead";
    String lastDeltaDir = lastDeltaMs < 0 ? "behind" : "ahead";
    boolean pacingConfigured = _configuration.isTimePacingEnabled();
    boolean pacingEffective = pacingConfigured
        && !_configuration.isDisableThrottling();
    String message = new StringBuilder(256)
        .append("Timing summary (last ")
        .append(intervalSeconds)
        .append("s)\n")
        .append("  Fiber shots: ")
        .append(summary.getTotalPackages())
        .append(" (drops: ")
        .append(summary.getDropCount())
        .append(")\n")
        .append("  Packages per second: ")
        .append(String.format("%.1f", packagesPerSecond))
        .append("\n")
        .append("  Delta vs wall clock: avg ")
        .append(String.format("%.1f", Math.abs(avgDeltaMs)))
        .append(" ms ")
        .append(avgDeltaDir)
        .append(", last ")
        .append(String.format("%.1f", Math.abs(lastDeltaMs)))
        .append(" ms ")
        .append(lastDeltaDir)
        .append("\n")
        .append("  Lagging fiber shots: ")
        .append(summary.getLagCount())
        .append(" (avg ")
        .append(String.format("%.1f", avgLagMs))
        .append(" ms, max ")
        .append(summary.getMaxLagNanos() / Helpers.millisInNano)
        .append(" ms)\n")
        .append("  Leading fiber shots: ")
        .append(summary.getLeadCount())
        .append(" (avg ")
        .append(String.format("%.1f", avgLeadMs))
        .append(" ms, max ")
        .append(summary.getMaxLeadNanos() / Helpers.millisInNano)
        .append(" ms)\n")
        .append("  Pacing: configured=")
        .append(pacingConfigured)
        .append(", effective=")
        .append(pacingEffective)
        .append(" (disableThrottling=")
        .append(_configuration.isDisableThrottling())
        .append(")")
        .append(" (sleeps ")
        .append(summary.getSleepCount())
        .append(", avg ")
        .append(String.format("%.1f", avgSleepMs))
        .append(" ms")
        .append(", pre-sleep lead avg ")
        .append(String.format("%.1f", avgPreSleepLeadMs))
        .append(" ms)\n")
        .append("  Package duration: ")
        .append(
          String.format("%.1f", _stepCalculator.nanosPrPackage() / 1_000_000.0)
        )
        .append(" ms")
        .toString();
    _logger.info(message);
  }

  private PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> constructAvroObjects(
      int currentLocus,
      List<Float> floatData,
      List<Long> longData
  ) {
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

  private static SimulatorBoxUnitConfiguration copyConfiguration(
      SimulatorBoxUnitConfiguration source) {
    SimulatorBoxUnitConfiguration copy = new SimulatorBoxUnitConfiguration();
    copy.setBoxUUID(source.getBoxUUID());
    copy.setOpticalPathUUID(source.getOpticalPathUUID());
    copy.setGaugeLength(source.getGaugeLength());
    copy.setSpatialSamplingInterval(source.getSpatialSamplingInterval());
    copy.setPulseWidth(source.getPulseWidth());
    copy.setStartLocusIndex(source.getStartLocusIndex());
    copy.setPulseRate(source.getPulseRate());
    copy.setMaxFreq(source.getMaxFreq());
    copy.setMinFreq(source.getMinFreq());
    copy.setNumberOfLoci(source.getNumberOfLoci());
    copy.setConversionConstant(source.getConversionConstant());
    copy.setDisableThrottling(source.isDisableThrottling());
    copy.setAmplitudesPrPackage(source.getAmplitudesPrPackage());
    copy.setNumberOfPrePopulatedValues(source.getNumberOfPrePopulatedValues());
    copy.setNumberOfShots(source.getNumberOfShots());
    copy.setSecondsToRun(source.getSecondsToRun());
    copy.setStartTimeEpochSecond(source.getStartTimeEpochSecond());
    copy.setAmplitudeDataType(source.getAmplitudeDataType());
    copy.setTimePacingEnabled(source.isTimePacingEnabled());
    copy.setTimeLagWarnMillis(source.getTimeLagWarnMillis());
    copy.setTimeLagDropMillis(source.getTimeLagDropMillis());
    return copy;
  }
}
