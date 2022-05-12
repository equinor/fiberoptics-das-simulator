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
package com.equinor.fiberoptics.das;

import com.equinor.fiberoptics.das.producer.variants.PackageStepCalculator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class PackageStepCalculatorTest {

  @Test
  public void testInitNonPow2() {
    IllegalArgumentException thrownException = assertThrows(
      IllegalArgumentException.class,
      () -> new PackageStepCalculator(0L,
        5000, 8193, 1)
    );
    assertTrue(thrownException.getMessage().contains("is not a power of 2"));
  }

  @Test
  public void testInit_with_5000hz_8192package() {
    PackageStepCalculator stepCal = new PackageStepCalculator(0L,
      5000, 8192, 1);
    assertEquals(0.8192d, stepCal.secondsPrPackage(), 0f);
    assertEquals(819.2d, stepCal.millisPrPackage(), 0f);
  }

  @Test
  public void testInit_with_2500hz_8192package() {
    PackageStepCalculator stepCal = new PackageStepCalculator(0L,
      2500, 8192, 1);
    assertEquals(1.6384d, stepCal.secondsPrPackage(), 0f);
    assertEquals(1638.4d, stepCal.millisPrPackage(), 0f);
  }

  @Test
  public void testInit_with_1000hz_16384package() {
    PackageStepCalculator stepCal = new PackageStepCalculator(0L,
      1000, 16384, 1);
    assertEquals(8.192d, stepCal.secondsPrPackage(), 0f);
    assertEquals(8192d, stepCal.millisPrPackage(), 0f);
  }

  @Test
  public void testStepping() {
    PackageStepCalculator stepCal = new PackageStepCalculator(0L,
      5000, 8192, 1);
    stepCal.increment(10);
    assertEquals(8_192_000_000L, stepCal.currentEpochNanos(), "Unexpected nano value");
    assertEquals(8_192_000_000L / 1_000_000, stepCal.currentEpochMillis(), "Unexpected millis value");
  }

  @Test
  public void testMessagesTotal() {
    PackageStepCalculator stepCal = new PackageStepCalculator(0L,
      5000, 8192, 5);
    stepCal.increment(10);
    assertEquals(50L, stepCal.getTotalMessages(), "Unexpected number of messages value");
  }

}
