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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * This is an example DAS  box unit implementation.
 * It's role is to convert the raw DAS data into a format that can be accepted into the Kafka server environment.
 * Serving the amplitude data
 *
 * @author Espen Tjonneland, espen@tjonneland.no
 */
@Component("SimulatorBoxUnit")
@EnableConfigurationProperties({ SimulatorBoxUnitConfiguration.class})
public class SimulatorBoxUnit implements GenericDasProducer {
  private static final Logger logger = LoggerFactory.getLogger(SimulatorBoxUnit.class);

  private final SimulatorBoxUnitConfiguration _configuration;
  private final PackageStepCalculator _stepCalculator;

  public SimulatorBoxUnit(SimulatorBoxUnitConfiguration configuration)
  {
    this._configuration = configuration;
    this._stepCalculator = new PackageStepCalculator(Instant.now(),
      _configuration.getMaxFreq(), _configuration.getAmplitudesPrPackage(), _configuration.getNumberOfLoci());
  }

  @Override
  public PackageStepCalculator getStepCalculator() {
    return _stepCalculator;
  }

  @Override
  public Flux<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> produce() {
    RandomDataCache dataCache = new RandomDataCache(_configuration.getNumberOfPrePopulatedValues(), _configuration.getAmplitudesPrPackage(), _configuration.getPulseRate());
    long delay = _configuration.isDisableThrottling () ? 0 : (long)_stepCalculator.millisPrPackage();

    logger.info(String.format("Starting to produce data now for %d seconds", _configuration.getSecondsToRun()));
    return Flux
        .interval(Duration.ofMillis(delay))
        .take(_configuration.getSecondsToRun())
        .map(tick -> {
          List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>> data = new ArrayList<>();

          for (int currentLocus = 0; currentLocus < _configuration.getNumberOfLoci(); currentLocus++) {
              var message = constructAvroObjects(currentLocus, dataCache.getFloat());
              data.add(message);
          }

          _stepCalculator.increment(1);
          return data;
        });
  }

  private PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> constructAvroObjects(int currentLocus, List<Float> data) {
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
