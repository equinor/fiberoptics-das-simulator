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
package com.equinor.fiberoptics.das.producer.variants.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimingStatsTest {

  @Test
  public void recordAndSnapshot_tracksLeadLagAndSleep() {
    TimingStats stats = new TimingStats();

    stats.record(5, 2, false, false, 3);
    stats.record(-7, 0, true, true);

    TimingStats.Summary summary = stats.snapshotAndReset();
    assertEquals(2, summary.getTotalPackages());
    assertEquals(1, summary.getLeadCount());
    assertEquals(1, summary.getLagCount());
    assertEquals(5, summary.getTotalLeadNanos());
    assertEquals(7, summary.getTotalLagNanos());
    assertEquals(5, summary.getMaxLeadNanos());
    assertEquals(7, summary.getMaxLagNanos());
    assertEquals(-2, summary.getTotalDeltaNanos());
    assertEquals(-7, summary.getLastDeltaNanos());
    assertEquals(3, summary.getTotalPreSleepLeadNanos());
    assertEquals(1, summary.getPreSleepLeadCount());
    assertEquals(1, summary.getSleepCount());
    assertEquals(2, summary.getTotalSleepNanos());
    assertEquals(1, summary.getDropCount());
    assertEquals(1, summary.getWarnLagCount());

    TimingStats.Summary resetSummary = stats.snapshotAndReset();
    assertEquals(0, resetSummary.getTotalPackages());
    assertEquals(0, resetSummary.getLeadCount());
    assertEquals(0, resetSummary.getLagCount());
    assertEquals(0, resetSummary.getTotalLeadNanos());
    assertEquals(0, resetSummary.getTotalLagNanos());
    assertEquals(0, resetSummary.getSleepCount());
    assertEquals(0, resetSummary.getDropCount());
    assertEquals(0, resetSummary.getWarnLagCount());
    assertEquals(-7, resetSummary.getLastDeltaNanos());
  }
}
