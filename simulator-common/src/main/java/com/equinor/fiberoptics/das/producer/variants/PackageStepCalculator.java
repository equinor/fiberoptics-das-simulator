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
package com.equinor.fiberoptics.das.producer.variants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;

import static com.equinor.fiberoptics.das.producer.variants.util.Helpers.millisInNano;
import static com.equinor.fiberoptics.das.producer.variants.util.Helpers.nanosInSecond;

/**
 * Used to calculate correct epochs and time interval for fiber vector shots.
 */
public class PackageStepCalculator {

  private static final Logger logger = LoggerFactory.getLogger(PackageStepCalculator.class);

  private final long _nanosPrPackage;
  private long _currentTimePointNanos;
  private final int _loci;

  private long _currentStep;

  /**
   * @param startTimeEpochNano  The nanosecond start point of the first DAS data package. Note that this is not the same
   *                            time that you start an Acquisition (as that also handles some none time deterministic setup
   *                            of Kafka topics etc.)
   * @param maximumFrequency    This is the Nyquist frequency of the amplitudes.
   * @param amplitudesPrPackage This is the number of data points pr Kafka package. Note it should always be a power of two number.
   * @param loci                Is the number of data channels that we are producing data on along the fiber.
   */
  public PackageStepCalculator(long startTimeEpochNano, float maximumFrequency, int amplitudesPrPackage, int loci) {
    isPow2(amplitudesPrPackage);

    _nanosPrPackage = (long) (nanosInSecond / ((maximumFrequency * 2) / amplitudesPrPackage));
    _currentTimePointNanos = startTimeEpochNano;
    _loci = loci;
    _currentStep = 0;

    logger.info("Package step calculator initialized for start: {}, maximumFrequency, {}, amplitudesPrPackage:{}, loci: {}",
      Instant.ofEpochMilli(startTimeEpochNano / 1_000_000), maximumFrequency, amplitudesPrPackage, loci);
  }

  /**
   * @param startTime           The start point of the first DAS data package. Note that this is not the same
   *                            time that you start an Acquisition (as that also handles some none time deterministic setup
   *                            of Kafka topics etc.)
   * @param maximumFrequency    This is the Nyquist frequency of the amplitudes.
   * @param amplitudesPrPackage This is the number of datapoints pr Kafka package. Note it should always be a power of two number.
   * @param loci                Is the number of data channels that we are producing data on along the fiber.
   */
  public PackageStepCalculator(Instant startTime, float maximumFrequency, int amplitudesPrPackage, int loci) {
    this(startTime.toEpochMilli() * millisInNano, maximumFrequency, amplitudesPrPackage, loci);
  }


  private void isPow2(int number) {
    boolean isPow2 = number > 0 && ((number & (number - 1)) == 0);
    if (!isPow2) {
      throw new IllegalArgumentException("The number: " + number + " is not a power of 2.");
    }
  }

  public Instant currentStepTime() {
    return Instant.ofEpochMilli(_currentTimePointNanos);
  }

  public double secondsPrPackage() {
    double x = _nanosPrPackage;
    x /= 1_000_000_000;
    return x;
  }

  public double millisPrPackage() {
    double x = _nanosPrPackage;
    x /= 1_000_000;
    return x;
  }

  public void increment(int times) {
    logger.debug("Update timepoint: {}", Instant.ofEpochMilli(_currentTimePointNanos / millisInNano));
    _currentTimePointNanos += (_nanosPrPackage * times);
    _currentStep += times;
    logger.debug("New timepoint: {}", Instant.ofEpochMilli(_currentTimePointNanos / millisInNano));
  }

  public long currentEpochNanos() {
    return _currentTimePointNanos;
  }

  public long currentEpochMillis() {
    return new BigDecimal(_currentTimePointNanos).movePointLeft(6).longValue();
  }

  public long getTotalMessages() {
    return _currentStep * _loci;
  }
}
