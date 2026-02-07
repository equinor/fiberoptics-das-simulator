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

import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HelpersTest {

  @Test
  public void currentEpochNanos_isMonotonicAcrossCalls() {
    long first = Helpers.currentEpochNanos();
    long second = Helpers.currentEpochNanos();

    assertTrue(second >= first);
  }

  @Test
  public void sleepNanos_noopForNonPositiveValues() {
    assertDoesNotThrow(() -> Helpers.sleepNanos(0));
    assertDoesNotThrow(() -> Helpers.sleepNanos(-1));
  }

  @Test
  public void sleepMillis_throwsWhenInterrupted() {
    Thread.currentThread().interrupt();
    try {
      assertThrows(RuntimeException.class, () -> Helpers.sleepMillis(1));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void wait_throwsWhenInterrupted() {
    CountDownLatch latch = new CountDownLatch(1);
    Thread.currentThread().interrupt();
    try {
      assertThrows(RuntimeException.class, () -> Helpers.wait(latch));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }
}
