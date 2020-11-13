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
package com.equinor.fiberoptics.das.producer.variants.simulatorboxunit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A very simple data-source that provides random(ish) data.
 *
 *  @author Espen Tjonneland, espen@tjonneland.no
 */
public class RandomDataCache {

  private final Map<Integer, List<Float>> _amplitudesPrLocus;
  private final int _amplitudesPrPackage;
  private final int _pulseRate;
  private int _currentIndex = 0;
  private final int _numberOfPrepoluatedValues;

  private static final Logger logger = LoggerFactory.getLogger(RandomDataCache.class);

  public RandomDataCache(int numberOfPrepoluatedValues, int amplitudesPrPackage, int pulseRate) {
    _amplitudesPrPackage = amplitudesPrPackage;
    _pulseRate = pulseRate;
    _numberOfPrepoluatedValues = numberOfPrepoluatedValues;
    _amplitudesPrLocus = prepareFloatEntries();
  }

  private Map<Integer, List<Float>> prepareFloatEntries() {
    logger.info("Pre-populating {} buffer values.", _numberOfPrepoluatedValues);
    Map<Integer, List<Float>> toReturn = new HashMap();
    for (int i = 0; i < _numberOfPrepoluatedValues; i++) {
      toReturn.put(i, getAmplitudesFloat(i));
    }
    logger.info("Done.");
    return toReturn;
  }

  public List<Float> getFloat() {
    if (_currentIndex >= _numberOfPrepoluatedValues) {
      _currentIndex = 0;
    }
    return _amplitudesPrLocus.get(_currentIndex++);

  }

  private List<Float> getAmplitudesFloat(long timeIndex) {

    List<Float> toReturn = new ArrayList<>();
    Random myRand = new Random();
    for (int currentTimeIndex = 0; currentTimeIndex < _amplitudesPrPackage; currentTimeIndex++) {
      double val =
        Math.sin((double) timeIndex / _pulseRate * myRand.nextFloat() * 2 * Math.PI);

      toReturn.add((float) val);
    }
    return toReturn;
  }

  /*private List<Long> getAmplitudeListLong(long timePoint) {

    List<Long> toReturn = new ArrayList<>();

    Random myRand = new Random();

    for (int currentTimePoint = 0; currentTimePoint < _amplitudesPrPackage; currentTimePoint++) {
      double val =
        Math.sin((double) timePoint / _pulseRate * myRand.nextFloat() * 2 * Math.PI);
      toReturn.add((long) (val * _simConfig.getConversionConstant()));
    }
    return toReturn;
  }*/


}
