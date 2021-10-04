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
import com.equinor.fiberoptics.das.producer.variants.util.Helpers;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

@ActiveProfiles("test")
@SpringBootTest(classes=StaticDataUnit.class)
@RunWith(SpringRunner.class)
public class StaticDataUnitTest {

  private static final Logger logger = LoggerFactory.getLogger(StaticDataUnitTest.class);
  private final static long millisInNano = 1_000_000;

  @Autowired
  StaticDataUnit staticDataUnit;

  @Test
  public void testStreamFromStaticDataBox() {

    Consumer<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> logOutput = value -> {
      for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry: value) {
        DASMeasurement measurement = entry.value;
        LocalDateTime ldt = Instant.ofEpochMilli(measurement.getStartSnapshotTimeNano() / millisInNano).atZone(ZoneId.systemDefault()).toLocalDateTime();

        logger.info("Locus {} with {} has {} amplitudes", measurement.getLocus(), ldt, measurement.getAmplitudesFloat().size());
        assertEquals("Number of amplitudes is as configured", 8192, measurement.getAmplitudesFloat().size());

        // Amplitudes are, for all loci values from 0 to 8191
        Float firstAmplitude = measurement.getAmplitudesFloat().get(0);
        Float lastAmplitude = measurement.getAmplitudesFloat().get(8191);

        assertEquals("firstAmplitude is expected", 0.0, firstAmplitude.floatValue(), 0);
        assertEquals("lastAmplitude is expected", 8191.0, lastAmplitude.floatValue(), 0);
      }
      assertEquals("Number of loci is as configured", 3, value.size());
    };

    CountDownLatch latch = new CountDownLatch(1);
    staticDataUnit.produce()
      .subscribe(logOutput,
      (ex) -> logger.info("Error emitted: " + ex.getMessage()),
        latch::countDown);

    Helpers.wait(latch);

  }
}
