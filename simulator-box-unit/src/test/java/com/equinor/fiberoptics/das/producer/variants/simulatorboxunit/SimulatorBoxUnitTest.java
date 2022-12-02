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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.springframework.test.util.AssertionErrors.assertEquals;

@ActiveProfiles("test")
@SpringBootTest(classes = SimulatorBoxUnit.class)
public class SimulatorBoxUnitTest {

  private static final Logger logger = LoggerFactory.getLogger(SimulatorBoxUnitTest.class);
  private final static long millisInNano = 1_000_000;

  @Autowired
  SimulatorBoxUnit simulatorBoxUnit;


  @Test
  public void testStreamFromSimulatorBox() {
    AtomicInteger consumed = new AtomicInteger();

    Consumer<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> logOutput = value -> {
      for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry : value) {
        DASMeasurement measurement = entry.value;
        LocalDateTime ldt = Instant.ofEpochMilli(measurement.getStartSnapshotTimeNano() / millisInNano).atZone(ZoneId.systemDefault()).toLocalDateTime();

        if (measurement.getAmplitudesFloat() != null) {
          logger.info("Locus {} with {} has {} float based amplitudes", measurement.getLocus(), ldt, measurement.getAmplitudesFloat().size());
        } else {
          logger.info("Locus {} with {} has {} long based amplitudes", measurement.getLocus(), ldt, measurement.getAmplitudesLong().size());
        }
      }

      assertEquals("Number of loci is as configured", 3, value.size());
      consumed.getAndIncrement();
    };

    CountDownLatch latch = new CountDownLatch(1);
    simulatorBoxUnit.produce()
      .subscribe(logOutput,
        (ex) -> logger.info("Error emitted: " + ex.getMessage()),
        latch::countDown);

    Helpers.wait(latch);

    assertEquals("Number consumed is as configured", 10, consumed.get());
  }
}
