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

package com.equinor.fiberoptics.das.producer.variants.staticdataunit;

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
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
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
 * This is a static data unit for testing that data is flowing as expected on the platform,
 * so that we can make asserts on the data flowing out and perform black-box testing
 *
 * @author Inge Knudsen, iknu@equinor.com
 */
@Component("StaticDataUnit")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@EnableConfigurationProperties({StaticDataUnitConfiguration.class})
public class StaticDataUnit implements GenericDasProducer {
  private static final Logger _logger = LoggerFactory.getLogger(StaticDataUnit.class);
  private static final Duration _TIMING_LOG_INTERVAL = Duration.ofSeconds(30);

  private final StaticDataUnitConfiguration _configuration;
  private final PackageStepCalculator _stepCalculator;

  public StaticDataUnit(StaticDataUnitConfiguration configuration) {
    _configuration = configuration;
    _stepCalculator = new PackageStepCalculator(
      _configuration.getStartTimeInstant(),
      _configuration.getMaxFreq(),
      _configuration.getAmplitudesPrPackage(),
      _configuration.getNumberOfLoci()
    );
  }

  @Override
  public Flux<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> produce() {
    Duration delay = _configuration.isDisableThrottling()
      ? Duration.ZERO
      : Duration.ofNanos(_stepCalculator.nanosPrPackage());
    if (_configuration.isDisableThrottling() && _configuration.isTimePacingEnabled()) {
      _logger.info("disableThrottling=true: pacing and drop-to-catch-up are disabled.");
    }
    long take;
    if (_configuration.getNumberOfShots() != null && _configuration.getNumberOfShots() > 0) {
      take = _configuration.getNumberOfShots();
      _logger.info(String.format("Starting to produce %d data", take));
    } else {
      take = delay.isZero()
        ? _configuration.getSecondsToRun() * 1000
        : (long) (
            _configuration.getSecondsToRun()
                / (delay.toNanos() / 1_000_000_000.0)
          );
        _logger.info(
          String.format(
              "Starting to produce data now for %d seconds",
              _configuration.getSecondsToRun()
          )
      );
    }
    final long takeFinal = take;

    return Flux.defer(() -> {
      TimingStats timingStats = new TimingStats();
      Scheduler timingScheduler = Schedulers.newSingle("staticdata-timing-logger");
        Disposable timingDisposable = Flux.interval(_TIMING_LOG_INTERVAL, timingScheduler)
          .doOnNext(tick -> logTimingSummary(timingStats))
          .subscribe();
      Scheduler producerScheduler = Schedulers.newSingle("staticdata-producer");
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
    boolean warnLag = deltaNanos < 0 && warnNanos > 0 && (-deltaNanos) > warnNanos;
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
    emitData(sink);
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
    emitData(sink);
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
      FluxSink<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> sink
  ) {
    List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>> data;
    if ("float".equalsIgnoreCase(_configuration.getAmplitudeDataType())) {
      List<Float> floatData = DoubleStream.iterate(0, i -> i + 1)
          .limit(_configuration.getAmplitudesPrPackage())
          .boxed()
          .map(Double::floatValue)
          .collect(Collectors.toList());

      data = IntStream.range(0, _configuration.getNumberOfLoci())
          .mapToObj(currentLocus -> constructFloatAvroObjects(currentLocus, floatData))
          .collect(Collectors.toList());
    } else {
      List<Long> longData = LongStream.iterate(0, i -> i + 1)
          .limit(_configuration.getAmplitudesPrPackage())
          .boxed()
          .collect(Collectors.toList());

      data = IntStream.range(0, _configuration.getNumberOfLoci())
          .mapToObj(currentLocus -> constructLongAvroObjects(currentLocus, longData))
          .collect(Collectors.toList());
    }
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
      return new PaceDecision(delayNanos, false, false, delayNanos, targetEpochNanos);
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
    if (summary.totalPackages == 0) {
      return;
    }
    double avgLagMs = summary.lagCount == 0
        ? 0
        : (summary.totalLagNanos / 1_000_000.0) / summary.lagCount;
    double avgLeadMs = summary.leadCount == 0
        ? 0
        : (summary.totalLeadNanos / 1_000_000.0) / summary.leadCount;
    double avgSleepMs = summary.sleepCount == 0
        ? 0
        : (summary.totalSleepNanos / 1_000_000.0) / summary.sleepCount;
    double avgPreSleepLeadMs = summary.preSleepLeadCount == 0
        ? 0
        : (summary.totalPreSleepLeadNanos / 1_000_000.0) / summary.preSleepLeadCount;
    double avgDeltaMs = summary.totalPackages == 0
        ? 0
        : (summary.totalDeltaNanos / 1_000_000.0) / summary.totalPackages;
    double lastDeltaMs = summary.lastDeltaNanos / 1_000_000.0;
    long intervalSeconds = Math.max(1, _TIMING_LOG_INTERVAL.getSeconds());
    double packagesPerSecond = summary.totalPackages / (double) intervalSeconds;
    String avgDeltaDir = avgDeltaMs < 0 ? "behind" : "ahead";
    String lastDeltaDir = lastDeltaMs < 0 ? "behind" : "ahead";
    boolean pacingConfigured = _configuration.isTimePacingEnabled();
    boolean pacingEffective = pacingConfigured && !_configuration.isDisableThrottling();
    String message = new StringBuilder(256)
        .append("Timing summary (last ")
        .append(intervalSeconds)
        .append("s)\n")
        .append("  Fiber shots: ")
        .append(summary.totalPackages)
        .append(" (drops: ")
        .append(summary.dropCount)
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
        .append(summary.lagCount)
        .append(" (avg ")
        .append(String.format("%.1f", avgLagMs))
        .append(" ms, max ")
        .append(summary.maxLagNanos / Helpers.millisInNano)
        .append(" ms)\n")
        .append("  Leading fiber shots: ")
        .append(summary.leadCount)
        .append(" (avg ")
        .append(String.format("%.1f", avgLeadMs))
        .append(" ms, max ")
        .append(summary.maxLeadNanos / Helpers.millisInNano)
        .append(" ms)\n")
        .append("  Pacing: configured=")
        .append(pacingConfigured)
        .append(", effective=")
        .append(pacingEffective)
        .append(" (disableThrottling=")
        .append(_configuration.isDisableThrottling())
        .append(")")
        .append(" (sleeps ")
        .append(summary.sleepCount)
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

  private PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> constructLongAvroObjects(
      int currentLocus,
      List<Long> data
  ) {
    return new PartitionKeyValueEntry<>(
      DASMeasurementKey.newBuilder()
        .setLocus(currentLocus)
        .build(),
      DASMeasurement.newBuilder()
        .setStartSnapshotTimeNano(_stepCalculator.currentEpochNanos())
        .setTrustedTimeSource(true)
        .setLocus(currentLocus)
        .setAmplitudesLong(data)
        .build(),
      currentLocus);
  }

  private PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> constructFloatAvroObjects(
      int currentLocus,
      List<Float> data
  ) {
    return new PartitionKeyValueEntry<>(
      DASMeasurementKey.newBuilder()
        .setLocus(currentLocus)
        .build(),
      DASMeasurement.newBuilder()
        .setStartSnapshotTimeNano(_stepCalculator.currentEpochNanos())
        .setTrustedTimeSource(true)
        .setLocus(currentLocus)
        .setAmplitudesFloat(data)
        .build(),
      currentLocus);
  }
}
