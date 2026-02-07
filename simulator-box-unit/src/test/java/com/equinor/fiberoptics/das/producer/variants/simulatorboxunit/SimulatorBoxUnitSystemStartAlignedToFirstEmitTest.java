/*-
 * ========================LICENSE_START=================================
 * simulator-box-unit
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
package com.equinor.fiberoptics.das.producer.variants.simulatorboxunit;

import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import com.equinor.fiberoptics.das.producer.variants.util.Helpers;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimulatorBoxUnitSystemStartAlignedToFirstEmitTest {

  @Test
  void alignsSystemStartTimeToFirstEmission() throws Exception {
    SimulatorBoxUnitConfiguration configuration = new SimulatorBoxUnitConfiguration();
    configuration.setNumberOfLoci(1);
    configuration.setStartLocusIndex(0);
    configuration.setPulseRate(10_000);
    configuration.setMaxFreq(5_000);
    configuration.setMinFreq(0);
    configuration.setGaugeLength(10.209524f);
    configuration.setPulseWidth(100.50f);
    configuration.setAmplitudesPrPackage(8192);
    configuration.setNumberOfPrePopulatedValues(1);
    configuration.setNumberOfShots(1);
    configuration.setSecondsToRun(1);
    configuration.setAmplitudeDataType("long");

    configuration.setDisableThrottling(true);
    configuration.setTimePacingEnabled(false);
    configuration.setStartTimeEpochSecond(0); // system time

    SimulatorBoxUnit producer = new SimulatorBoxUnit(configuration);

    Thread.sleep(200);
    long beforeSubscribeNanos = Helpers.currentEpochNanos();

    List<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> batches =
      producer.produce().collectList().block(Duration.ofSeconds(5));

    assertNotNull(batches);
    long firstTimestamp = batches.get(0).get(0).getValue().getStartSnapshotTimeNano();
    assertTrue(firstTimestamp >= (beforeSubscribeNanos - (20 * Helpers.millisInNano)),
      "First package timestamp should be aligned to when production starts (not bean construction time)");
  }
}

