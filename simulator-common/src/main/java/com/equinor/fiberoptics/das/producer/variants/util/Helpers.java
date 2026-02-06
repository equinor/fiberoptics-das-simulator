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

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility helpers for timing and blocking operations.
 */
public class Helpers {

  private static final Logger _logger = LoggerFactory.getLogger(Helpers.class);
  public static final long millisInNano = 1_000_000;
  public static final long nanosInSecond = 1_000_000_000;

  /**
   * Returns the current epoch time in nanoseconds.
   */
  public static long currentEpochNanos() {
    Instant now = Instant.now();
    return (now.getEpochSecond() * nanosInSecond) + now.getNano();
  }

  /**
   * Sleeps for the given number of milliseconds.
   */
  public static void sleepMillis(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      _logger.error("Interrupted Thread");
      throw new RuntimeException("Interrupted thread");
    }
  }

  /**
   * Sleeps for the given number of nanoseconds.
   */
  public static void sleepNanos(long nanos) {
    if (nanos <= 0) {
      return;
    }
    long millis = nanos / millisInNano;
    int nanosPart = (int) (nanos - (millis * millisInNano));
    try {
      Thread.sleep(millis, nanosPart);
    } catch (InterruptedException e) {
      _logger.error("Interrupted Thread");
      throw new RuntimeException("Interrupted thread");
    }
  }

  /**
   * Blocks until the latch reaches zero.
   */
  public static void wait(CountDownLatch waitOn) {
    try {
      waitOn.await();
    } catch (InterruptedException e) {
      _logger.error("Interrupted waiting on CountDownLatch");
      throw new RuntimeException("Interrupted thread");
    }
  }

}
