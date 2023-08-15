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

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Instant;

/**
 * Configuration class for the DAS box unit
 *
 * @author Espen Tjonneland, espen@tjonneland.no
 */
@ConfigurationProperties(prefix = "das.producer.box.simulator")
@Getter
@Setter
public class SimulatorBoxUnitConfiguration {
  private String boxUUID;
  private String opticalPathUUID;
  private float gaugeLength;
  private float spatialSamplingInterval;
  private float pulseWidth;
  private int startLocusIndex;
  private int pulseRate;
  private float maxFreq;
  private float minFreq;
  private int numberOfLoci;
  private double conversionConstant;
  private boolean disableThrottling;
  private int amplitudesPrPackage;
  private int numberOfPrePopulatedValues;
  private Integer numberOfShots;
  private int secondsToRun;
  private long startTimeEpochSecond;
  private String amplitudeDataType;

  public Instant getStartTimeInstant() {
    return startTimeEpochSecond==0 ? Instant.now() : Instant.ofEpochSecond(startTimeEpochSecond);
  }

  @PostConstruct
  public void afterInit() {
    conversionConstant = ((maxFreq * 2) / gaugeLength) * 116;
  }

  public boolean isDisableThrottling() {
    return disableThrottling;
  }
}
