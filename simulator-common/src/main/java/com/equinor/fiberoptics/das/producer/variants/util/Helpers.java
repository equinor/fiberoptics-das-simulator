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
