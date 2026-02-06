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

import java.util.AbstractList;
import java.util.List;
import java.util.Random;
import java.util.RandomAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A very simple data-source that provides random(ish) data.
 *
 *  @author Espen Tjonneland, espen@tjonneland.no
 */
public class RandomDataCache {

  private final List<Float>[] _amplitudesPrLocusFloats;
  private final List<Long>[] _amplitudesPrLocusLong;
  private final int _amplitudesPrPackage;
  private final int _pulseRate;
  private int _currentIndex = 0;
  private final int _numberOfPrepoluatedValues;

  private static final Logger _logger = LoggerFactory.getLogger(RandomDataCache.class);

  /**
   * Initializes the cache with prepopulated data.
   */
  public RandomDataCache(
      int numberOfPrePopuluatedValues,
      int amplitudesPrPackage,
      int pulseRate,
      String dataType
  ) {
    _amplitudesPrPackage = amplitudesPrPackage;
    _pulseRate = pulseRate;
    _numberOfPrepoluatedValues = numberOfPrePopuluatedValues;
    if (dataType.equalsIgnoreCase("long")) {
      _amplitudesPrLocusFloats = null;
      _amplitudesPrLocusLong = prepareLongEntries();
    } else {
      _amplitudesPrLocusLong = null;
      _amplitudesPrLocusFloats = prepareFloatEntries();
    }
  }

  /**
   * Returns the next float-based sample list, or null if the cache is long-based.
   */
  public List<Float> getFloat() {
    if (_currentIndex >= _numberOfPrepoluatedValues) {
      _currentIndex = 0;
    }
    int index = _currentIndex++;
    if (_amplitudesPrLocusFloats == null) {
      return null;
    }
    return _amplitudesPrLocusFloats[index];
  }

  /**
   * Returns the next long-based sample list, or null if the cache is float-based.
   */
  public List<Long> getLong() {
    if (_currentIndex >= _numberOfPrepoluatedValues) {
      _currentIndex = 0;
    }
    int index = _currentIndex++;
    if (_amplitudesPrLocusLong == null) {
      return null;
    }
    return _amplitudesPrLocusLong[index];
  }

  private List<Long>[] prepareLongEntries() {
    _logger.info("Pre-populating {} long buffer values.", _numberOfPrepoluatedValues);
    @SuppressWarnings("unchecked")
    List<Long>[] toReturn = new List[_numberOfPrepoluatedValues];
    for (int i = 0; i < _numberOfPrepoluatedValues; i++) {
      toReturn[i] = new LongArrayList(getAmplitudesLong(i));
    }
    _logger.info("Done.");
    return toReturn;
  }

  private List<Float>[] prepareFloatEntries() {
    _logger.info("Pre-populating {} float buffer values.", _numberOfPrepoluatedValues);
    @SuppressWarnings("unchecked")
    List<Float>[] toReturn = new List[_numberOfPrepoluatedValues];
    for (int i = 0; i < _numberOfPrepoluatedValues; i++) {
      toReturn[i] = new FloatArrayList(getAmplitudesFloat(i));
    }
    _logger.info("Done.");
    return toReturn;
  }


  private long[] getAmplitudesLong(long timeIndex) {
    long[] toReturn = new long[_amplitudesPrPackage];
    Random myRand = new Random();
    for (int currentTimeIndex = 0; currentTimeIndex < _amplitudesPrPackage; currentTimeIndex++) {
      long val = (long) (
          Math.sin(myRand.nextLong() * 2 * Math.PI) * (_pulseRate + timeIndex)
      );
      // (long) Math.sin(timeIndex / _pulseRate * myRand.nextLong() * 2 * Math.PI);

      toReturn[currentTimeIndex] = val;
    }
    return toReturn;
  }

  private float[] getAmplitudesFloat(long timeIndex) {
    float[] toReturn = new float[_amplitudesPrPackage];
    Random myRand = new Random();
    for (int currentTimeIndex = 0; currentTimeIndex < _amplitudesPrPackage; currentTimeIndex++) {
      double val = Math.sin(
          (double) timeIndex / _pulseRate * myRand.nextFloat() * 2 * Math.PI
      );

      toReturn[currentTimeIndex] = (float) val;
    }
    return toReturn;
  }

  private static final class FloatArrayList extends AbstractList<Float> implements RandomAccess {
    private final float[] _data;

    private FloatArrayList(float[] data) {
      _data = data;
    }

    @Override
    public Float get(int index) {
      return _data[index];
    }

    @Override
    public int size() {
      return _data.length;
    }
  }

  private static final class LongArrayList extends AbstractList<Long> implements RandomAccess {
    private final long[] _data;

    private LongArrayList(long[] data) {
      _data = data;
    }

    @Override
    public Long get(int index) {
      return _data[index];
    }

    @Override
    public int size() {
      return _data.length;
    }
  }
}
