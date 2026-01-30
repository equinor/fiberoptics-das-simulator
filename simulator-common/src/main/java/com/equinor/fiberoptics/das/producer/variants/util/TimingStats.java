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

  public void record(long deltaNanos, long sleptNanos, boolean dropped, boolean warnLag) {
    record(deltaNanos, sleptNanos, dropped, warnLag, 0);
  }

  public void record(long deltaNanos, long sleptNanos, boolean dropped, boolean warnLag, long preSleepLeadNanos) {
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

  public Summary snapshotAndReset() {
    Summary summary = new Summary();
    summary.totalPackages = _totalPackages.getAndSet(0);
    summary.lagCount = _lagCount.getAndSet(0);
    summary.leadCount = _leadCount.getAndSet(0);
    summary.totalLagNanos = _totalLagNanos.getAndSet(0);
    summary.totalLeadNanos = _totalLeadNanos.getAndSet(0);
    summary.maxLagNanos = _maxLagNanos.getAndSet(0);
    summary.maxLeadNanos = _maxLeadNanos.getAndSet(0);
    summary.totalDeltaNanos = _totalDeltaNanos.getAndSet(0);
    summary.lastDeltaNanos = _lastDeltaNanos.get();
    summary.totalPreSleepLeadNanos = _totalPreSleepLeadNanos.getAndSet(0);
    summary.preSleepLeadCount = _preSleepLeadCount.getAndSet(0);
    summary.sleepCount = _sleepCount.getAndSet(0);
    summary.totalSleepNanos = _totalSleepNanos.getAndSet(0);
    summary.dropCount = _dropCount.getAndSet(0);
    summary.warnLagCount = _warnLagCount.getAndSet(0);
    return summary;
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

  public static class Summary {
    public long totalPackages;
    public long lagCount;
    public long leadCount;
    public long totalLagNanos;
    public long totalLeadNanos;
    public long maxLagNanos;
    public long maxLeadNanos;
    public long totalDeltaNanos;
    public long lastDeltaNanos;
    public long totalPreSleepLeadNanos;
    public long preSleepLeadCount;
    public long sleepCount;
    public long totalSleepNanos;
    public long dropCount;
    public long warnLagCount;
  }
}
