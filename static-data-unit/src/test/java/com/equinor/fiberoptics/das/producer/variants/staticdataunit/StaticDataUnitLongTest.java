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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ActiveProfiles("test")
@SpringBootTest(classes=StaticDataUnit.class)
@TestPropertySource(
  locations = {"classpath:application-test.yaml"},
  properties = {
    "AMPLITUDE_DATA_TYPE=long"
  })
public class StaticDataUnitLongTest {

  private static final Logger logger = LoggerFactory.getLogger(StaticDataUnitLongTest.class);
  private final static long millisInNano = 1_000_000;



  @Autowired
  StaticDataUnit staticDataUnit;

  @Test
  public void testStreamFromStaticDataBoxAsFLong() {

    Consumer<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> logOutput = value -> {
      for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry: value) {
        DASMeasurement measurement = entry.value;
        LocalDateTime ldt = Instant.ofEpochMilli(measurement.getStartSnapshotTimeNano() / millisInNano).atZone(ZoneId.systemDefault()).toLocalDateTime();

        logger.info("Locus {} with {} has {} amplitudes", measurement.getLocus(), ldt, measurement.getAmplitudesLong().size());
        assertEquals( 8192, measurement.getAmplitudesLong().size(), "Number of amplitudes is as configured");

        // Amplitudes are, for all loci values from 0 to 8191
        Long firstAmplitude = measurement.getAmplitudesLong().get(0);
        Long lastAmplitude = measurement.getAmplitudesLong().get(8191);

        assertEquals(0.0, firstAmplitude, 0, "firstAmplitude is expected");
        assertEquals(8191.0, lastAmplitude, 0, "lastAmplitude is expected");
      }
      assertEquals(3, value.size(), "Number of loci is as configured");
    };

    CountDownLatch latch = new CountDownLatch(1);
    staticDataUnit.produce()
      .subscribe(logOutput,
      (ex) -> logger.info("Error emitted: " + ex.getMessage()),
        latch::countDown);

    Helpers.wait(latch);

  }
}
