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
package com.equinor.fiberoptics.das.producer.variants.staticdataunit;

import com.equinor.fiberoptics.das.producer.variants.GenericDasProducer;
import com.equinor.fiberoptics.das.producer.variants.PackageStepCalculator;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * This is a static data unit for testing that data is flowing as expected on the platform, so that we can make asserts on the data flowing
 * out and perform  black-box testing
 *
 * @author Inge Knudsen, iknu@equinor.com
 */
@Component("StaticDataUnit")
@EnableConfigurationProperties({StaticDataUnitConfiguration.class})
public class StaticDataUnit implements GenericDasProducer {
  private static final Logger logger = LoggerFactory.getLogger(StaticDataUnit.class);

  private final StaticDataUnitConfiguration _configuration;
  private final PackageStepCalculator _stepCalculator;

  public StaticDataUnit(StaticDataUnitConfiguration configuration) {
    this._configuration = configuration;
    this._stepCalculator = new PackageStepCalculator(_configuration.getStartTimeInstant(),
      _configuration.getMaxFreq(), _configuration.getAmplitudesPrPackage(), _configuration.getNumberOfLoci());
  }

  @Override
  public Flux<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> produce() {
    long delay = _configuration.isDisableThrottling() ? 0 : (long) _stepCalculator.millisPrPackage();
    long take;
    if (_configuration.getNumberOfShots() != null && _configuration.getNumberOfShots() > 0) {
      take = _configuration.getNumberOfShots();
      logger.info(String.format("Starting to produce %d data", take));
    } else {
      take = delay == 0 ? _configuration.getSecondsToRun() * 1000 : (long) (_configuration.getSecondsToRun() / (delay / 1000.0));
      logger.info(String.format("Starting to produce data now for %d seconds", _configuration.getSecondsToRun()));
    }

    return Flux
      .interval(Duration.ofMillis(delay))
      .take(take)
      .map(tick -> {
        List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>> data;

        if ("float".equalsIgnoreCase(_configuration.getAmplitudeDataType())) {
          List<Float> floatData = DoubleStream.iterate(0, i -> i + 1)
            .limit(_configuration.getAmplitudesPrPackage())
            .boxed()
            .map(Double::floatValue)
            .collect(Collectors.toList());

          data = IntStream.range(0, _configuration.getNumberOfLoci())
            .mapToObj(currentLocus -> constructFloatAvroObjects(currentLocus, floatData))
            .collect(Collectors.toList());
        } else {
          List<Long> longData = LongStream.iterate(0, i -> i + 1)
            .limit(_configuration.getAmplitudesPrPackage())
            .boxed()
            .collect(Collectors.toList());

          data = IntStream.range(0, _configuration.getNumberOfLoci())
            .mapToObj(currentLocus -> constructLongAvroObjects(currentLocus, longData))
            .collect(Collectors.toList());

        }
        _stepCalculator.increment(1);
        return data;
      });
  }

  private PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> constructLongAvroObjects(int currentLocus, List<Long> data) {
    return new PartitionKeyValueEntry<>(
      DASMeasurementKey.newBuilder()
        .setLocus(currentLocus)
        .build(),
      DASMeasurement.newBuilder()
        .setStartSnapshotTimeNano(_stepCalculator.currentEpochNanos())
        .setTrustedTimeSource(true)
        .setLocus(currentLocus)
        .setAmplitudesLong(data)
        .build(),
      currentLocus);
  }

  private PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> constructFloatAvroObjects(int currentLocus, List<Float> data) {
    return new PartitionKeyValueEntry<>(
      DASMeasurementKey.newBuilder()
        .setLocus(currentLocus)
        .build(),
      DASMeasurement.newBuilder()
        .setStartSnapshotTimeNano(_stepCalculator.currentEpochNanos())
        .setTrustedTimeSource(true)
        .setLocus(currentLocus)
        .setAmplitudesFloat(data)
        .build(),
      currentLocus);
  }
}
