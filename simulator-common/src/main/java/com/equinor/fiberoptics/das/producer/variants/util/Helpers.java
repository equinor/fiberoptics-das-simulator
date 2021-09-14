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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class Helpers {

  private static final Logger logger = LoggerFactory.getLogger(Helpers.class);
  public final static long millisInNano = 1_000_000;
  public final static long nanosInSecond = 1_000_000_000;

  public static void sleepMillis(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      logger.error("Interrupted Thread");
      throw new RuntimeException("Interrupted thread");
    }
  }

  public static void wait(CountDownLatch waitOn) {
    try {
      waitOn.await();
    } catch (InterruptedException e) {
      logger.error("Interrupted waiting on CountDownLatch");
      throw new RuntimeException("Interrupted thread");
    }
  }

}
