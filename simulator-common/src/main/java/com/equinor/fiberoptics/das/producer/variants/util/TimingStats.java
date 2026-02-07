/*-
 * ========================LICENSE_START=================================
 * simulator-common
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

package com.equinor.fiberoptics.das.producer.variants.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects timing statistics for simulated packages.
 */
public class TimingStats {

  private final AtomicLong _totalPackages = new AtomicLong();
  private final AtomicLong _lagCount = new AtomicLong();
  private final AtomicLong _leadCount = new AtomicLong();
  private final AtomicLong _totalLagNanos = new AtomicLong();
  private final AtomicLong _totalLeadNanos = new AtomicLong();
  private final AtomicLong _maxLagNanos = new AtomicLong();
  private final AtomicLong _maxLeadNanos = new AtomicLong();
  private final AtomicLong _totalDeltaNanos = new AtomicLong();
  private final AtomicLong _lastDeltaNanos = new AtomicLong();
  private final AtomicLong _totalPreSleepLeadNanos = new AtomicLong();
  private final AtomicLong _preSleepLeadCount = new AtomicLong();
  private final AtomicLong _sleepCount = new AtomicLong();
  private final AtomicLong _totalSleepNanos = new AtomicLong();
  private final AtomicLong _dropCount = new AtomicLong();
  private final AtomicLong _warnLagCount = new AtomicLong();

  /**
   * Records timing stats without a pre-sleep lead value.
   */
  public void record(long deltaNanos, long sleptNanos, boolean dropped, boolean warnLag) {
    record(deltaNanos, sleptNanos, dropped, warnLag, 0);
  }

  /**
   * Records timing stats for a single package.
   */
  public void record(
      long deltaNanos,
      long sleptNanos,
      boolean dropped,
      boolean warnLag,
      long preSleepLeadNanos) {
    _totalPackages.incrementAndGet();
    _totalDeltaNanos.addAndGet(deltaNanos);
    _lastDeltaNanos.set(deltaNanos);
    if (preSleepLeadNanos > 0) {
      _totalPreSleepLeadNanos.addAndGet(preSleepLeadNanos);
      _preSleepLeadCount.incrementAndGet();
    }
    if (deltaNanos >= 0) {
      _leadCount.incrementAndGet();
      _totalLeadNanos.addAndGet(deltaNanos);
      updateMax(_maxLeadNanos, deltaNanos);
    } else {
      long lag = -deltaNanos;
      _lagCount.incrementAndGet();
      _totalLagNanos.addAndGet(lag);
      updateMax(_maxLagNanos, lag);
      if (warnLag) {
        _warnLagCount.incrementAndGet();
      }
    }
    if (sleptNanos > 0) {
      _sleepCount.incrementAndGet();
      _totalSleepNanos.addAndGet(sleptNanos);
    }
    if (dropped) {
      _dropCount.incrementAndGet();
    }
  }

  /**
   * Returns a summary snapshot and resets internal counters.
   */
  public Summary snapshotAndReset() {
    long totalPackages = _totalPackages.getAndSet(0);
    long lagCount = _lagCount.getAndSet(0);
    long leadCount = _leadCount.getAndSet(0);
    long totalLagNanos = _totalLagNanos.getAndSet(0);
    long totalLeadNanos = _totalLeadNanos.getAndSet(0);
    long maxLagNanos = _maxLagNanos.getAndSet(0);
    long maxLeadNanos = _maxLeadNanos.getAndSet(0);
    long totalDeltaNanos = _totalDeltaNanos.getAndSet(0);
    long lastDeltaNanos = _lastDeltaNanos.get();
    long totalPreSleepLeadNanos = _totalPreSleepLeadNanos.getAndSet(0);
    long preSleepLeadCount = _preSleepLeadCount.getAndSet(0);
    long sleepCount = _sleepCount.getAndSet(0);
    long totalSleepNanos = _totalSleepNanos.getAndSet(0);
    long dropCount = _dropCount.getAndSet(0);
    long warnLagCount = _warnLagCount.getAndSet(0);
    return new Summary(
        totalPackages,
        lagCount,
        leadCount,
        totalLagNanos,
        totalLeadNanos,
        maxLagNanos,
        maxLeadNanos,
        totalDeltaNanos,
        lastDeltaNanos,
        totalPreSleepLeadNanos,
        preSleepLeadCount,
        sleepCount,
        totalSleepNanos,
        dropCount,
        warnLagCount
    );
  }

  private static void updateMax(AtomicLong max, long candidate) {
    long current;
    do {
      current = max.get();
      if (candidate <= current) {
        return;
      }
    } while (!max.compareAndSet(current, candidate));
  }

  /**
   * Summary counters for a snapshot interval.
   */
  public static class Summary {
    private final long _totalPackages;
    private final long _lagCount;
    private final long _leadCount;
    private final long _totalLagNanos;
    private final long _totalLeadNanos;
    private final long _maxLagNanos;
    private final long _maxLeadNanos;
    private final long _totalDeltaNanos;
    private final long _lastDeltaNanos;
    private final long _totalPreSleepLeadNanos;
    private final long _preSleepLeadCount;
    private final long _sleepCount;
    private final long _totalSleepNanos;
    private final long _dropCount;
    private final long _warnLagCount;

    public Summary(
        long totalPackages,
        long lagCount,
        long leadCount,
        long totalLagNanos,
        long totalLeadNanos,
        long maxLagNanos,
        long maxLeadNanos,
        long totalDeltaNanos,
        long lastDeltaNanos,
        long totalPreSleepLeadNanos,
        long preSleepLeadCount,
        long sleepCount,
        long totalSleepNanos,
        long dropCount,
        long warnLagCount
    ) {
      _totalPackages = totalPackages;
      _lagCount = lagCount;
      _leadCount = leadCount;
      _totalLagNanos = totalLagNanos;
      _totalLeadNanos = totalLeadNanos;
      _maxLagNanos = maxLagNanos;
      _maxLeadNanos = maxLeadNanos;
      _totalDeltaNanos = totalDeltaNanos;
      _lastDeltaNanos = lastDeltaNanos;
      _totalPreSleepLeadNanos = totalPreSleepLeadNanos;
      _preSleepLeadCount = preSleepLeadCount;
      _sleepCount = sleepCount;
      _totalSleepNanos = totalSleepNanos;
      _dropCount = dropCount;
      _warnLagCount = warnLagCount;
    }

    public long getTotalPackages() {
      return _totalPackages;
    }

    public long getLagCount() {
      return _lagCount;
    }

    public long getLeadCount() {
      return _leadCount;
    }

    public long getTotalLagNanos() {
      return _totalLagNanos;
    }

    public long getTotalLeadNanos() {
      return _totalLeadNanos;
    }

    public long getMaxLagNanos() {
      return _maxLagNanos;
    }

    public long getMaxLeadNanos() {
      return _maxLeadNanos;
    }

    public long getTotalDeltaNanos() {
      return _totalDeltaNanos;
    }

    public long getLastDeltaNanos() {
      return _lastDeltaNanos;
    }

    public long getTotalPreSleepLeadNanos() {
      return _totalPreSleepLeadNanos;
    }

    public long getPreSleepLeadCount() {
      return _preSleepLeadCount;
    }

    public long getSleepCount() {
      return _sleepCount;
    }

    public long getTotalSleepNanos() {
      return _totalSleepNanos;
    }

    public long getDropCount() {
      return _dropCount;
    }

    public long getWarnLagCount() {
      return _warnLagCount;
    }
  }
}
