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

public class PackageStepCalculator {

  private static final Logger logger = LoggerFactory.getLogger(PackageStepCalculator.class);

  private final long _nanosPrPackage;
  private final double _secondsPrPackage;
  private final double _millisPrPackage;
  private long _currentTimePointNanos;
  private final int _loci;
  private long _currentStep;

  /**
   * Constructor to initialize the PackageStepCalculator.
   *
   * @param startTimeEpochNano   the start time in nanoseconds since epoch
   * @param maximumFrequency     the maximum frequency
   * @param amplitudesPrPackage  the number of amplitudes per package
   * @param loci                 the number of loci
   */
  public PackageStepCalculator(long startTimeEpochNano, float maximumFrequency, int amplitudesPrPackage, int loci) {
    if (amplitudesPrPackage <= 0 || (amplitudesPrPackage & (amplitudesPrPackage - 1)) != 0) {
      throw new IllegalArgumentException("The number: " + amplitudesPrPackage + " is not a power of 2.");
    }

    _nanosPrPackage = (long) (nanosInSecond / ((maximumFrequency * 2) / amplitudesPrPackage));
    _secondsPrPackage = _nanosPrPackage / 1_000_000_000.0;
    _millisPrPackage = _nanosPrPackage / 1_000_000.0;
    _currentTimePointNanos = startTimeEpochNano;
    _loci = loci;
    _currentStep = 0;

    logger.info("Package step calculator initialized for start: {}, maximumFrequency, {}, amplitudesPrPackage:{}, loci: {}",
      Instant.ofEpochMilli(startTimeEpochNano / 1_000_000), maximumFrequency, amplitudesPrPackage, loci);
  }

  /**
   * Constructor to initialize the PackageStepCalculator.
   *
   * @param startTime            the start time as an Instant
   * @param maximumFrequency     the maximum frequency
   * @param amplitudesPrPackage  the number of amplitudes per package
   * @param loci                 the number of loci
   */
  public PackageStepCalculator(Instant startTime, float maximumFrequency, int amplitudesPrPackage, int loci) {
    this(startTime.toEpochMilli() * millisInNano, maximumFrequency, amplitudesPrPackage, loci);
  }

  /**
   * Returns the current step time as an Instant.
   *
   * @return the current step time
   */
  public Instant currentStepTime() {
    return Instant.ofEpochMilli(_currentTimePointNanos);
  }

  /**
   * Returns the duration of each package in seconds.
   *
   * @return the duration of each package in seconds
   */
  public double secondsPrPackage() {
    return _secondsPrPackage;
  }

  /**
   * Returns the duration of each package in milliseconds.
   *
   * @return the duration of each package in milliseconds
   */
  public double millisPrPackage() {
    return _millisPrPackage;
  }

  /**
   * Increments the current time point by a specified number of steps.
   *
   * @param times the number of steps to increment
   */
  public void increment(int times) {
    if (logger.isDebugEnabled()) {
      logger.debug("Update timepoint: {}", Instant.ofEpochMilli(_currentTimePointNanos / millisInNano));
    }
    _currentTimePointNanos += (_nanosPrPackage * times);
    _currentStep += times;
    if (logger.isDebugEnabled()) {
      logger.debug("New timepoint: {}", Instant.ofEpochMilli(_currentTimePointNanos / millisInNano));
    }
  }

  /**
   * Returns the current time point in nanoseconds.
   *
   * @return the current time point in nanoseconds
   */
  public long currentEpochNanos() {
    return _currentTimePointNanos;
  }

  /**
   * Returns the current time point in milliseconds.
   *
   * @return the current time point in milliseconds
   */
  public long currentEpochMillis() {
    return new BigDecimal(_currentTimePointNanos).movePointLeft(6).longValue();
  }

  /**
   * Calculates the total number of messages based on the current step and the number of loci.
   *
   * @return the total number of messages
   */
  public long getTotalMessages() {
    return _currentStep * _loci;
  }
}
