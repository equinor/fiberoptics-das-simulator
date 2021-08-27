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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

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
  @Autowired
  private SimulatorBoxUnitConfiguration configuration;

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public Supplier<Flux<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> produce() {
    RandomDataCache dataCache = new RandomDataCache(100, 8, configuration.getPulseRate());

    PackageStepCalculator stepCalculator = new PackageStepCalculator(Instant.now(),
      configuration.getMaxFreq(), 8, configuration.getNumberOfLoci());

    return () ->
      Flux.range(0, 10)
        .map(index ->
          constructAvroObjects(stepCalculator, index, dataCache.getFloat())
        )
        .log();
  }

  private PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> constructAvroObjects(PackageStepCalculator stepCalculator, int currentLocus, List<Float> data) {
    return new PartitionKeyValueEntry<>(
      DASMeasurementKey.newBuilder()
        .setLocus(currentLocus)
        .build(),
      DASMeasurement.newBuilder()
        .setStartSnapshotTimeNano(stepCalculator.currentEpochNanos())
        .setTrustedTimeSource(true)
        .setLocus(currentLocus)
        .setAmplitudesFloat(data)
        .build(),
      currentLocus);
  }
}
