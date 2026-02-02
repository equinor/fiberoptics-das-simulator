/*-
 * ========================LICENSE_START=================================
 * static-data-unit
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
package com.equinor.fiberoptics.das.producer.variants.staticdataunit;

import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StaticDataUnitDisableThrottlingNoDropTest {

  @Test
  void doesNotDropPackagesWhenDisableThrottlingIsEnabled() {
    StaticDataUnitConfiguration configuration = new StaticDataUnitConfiguration();
    configuration.setNumberOfLoci(1);
    configuration.setAmplitudesPrPackage(16);
    configuration.setMaxFreq(5_000);
    configuration.setNumberOfShots(25);
    configuration.setSecondsToRun(1);

    configuration.setDisableThrottling(true);
    configuration.setTimePacingEnabled(true);
    configuration.setTimeLagDropMillis(1);
    configuration.setTimeLagWarnMillis(0);
    configuration.setStartTimeEpochSecond(0);
    configuration.setAmplitudeDataType("float");

    StaticDataUnit producer = new StaticDataUnit(configuration);

    List<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> batches =
      producer.produce().collectList().block(Duration.ofSeconds(5));

    assertNotNull(batches);
    assertTrue(batches.stream().allMatch(batch -> batch.size() == 1),
      "disableThrottling=true should not emit empty drop batches");
  }
}

