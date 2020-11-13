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

import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.annotation.PostConstruct;

/**
 * Configuration class for the DAS box unit
 *
 * @author Espen Tjonneland, espen@tjonneland.no
 */
@ConfigurationProperties(prefix = "das.producer.box.simulator")
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
  private boolean disableThrotteling;
  private int secondsToRun;

  @PostConstruct
  public void afterInit() {
    conversionConstant = ((maxFreq * 2) / gaugeLength) * 116;
  }

  public float getMinFreq() {
    return minFreq;
  }

  public void setMinFreq(float minFreq) {
    this.minFreq = minFreq;
  }

  public void setConversionConstant(double conversionConstant) {
    this.conversionConstant = conversionConstant;
  }

  public boolean isDisableThrotteling() {
    return disableThrotteling;
  }

  public void setDisableThrotteling(boolean disableThrotteling) {
    this.disableThrotteling = disableThrotteling;
  }

  public double getConversionConstant() {
    return conversionConstant;
  }

  public int getPulseRate() {
    return pulseRate;
  }

  public void setPulseRate(int pulseRate) {
    this.pulseRate = pulseRate;
  }

  public float getMaxFreq() {
    return maxFreq;
  }

  public void setMaxFreq(float maxFreq) {
    this.maxFreq = maxFreq;
  }

  public String getBoxUUID() {
    return boxUUID;
  }

  public void setBoxUUID(String boxUUID) {
    this.boxUUID = boxUUID;
  }

  public String getOpticalPathUUID() {
    return opticalPathUUID;
  }

  public void setOpticalPathUUID(String opticalPathUUID) {
    this.opticalPathUUID = opticalPathUUID;
  }

  public float getGaugeLength() {
    return gaugeLength;
  }

  public void setGaugeLength(float gaugeLength) {
    this.gaugeLength = gaugeLength;
  }

  public float getSpatialSamplingInterval() {
    return spatialSamplingInterval;
  }

  public void setSpatialSamplingInterval(float spatialSamplingInterval) {
    this.spatialSamplingInterval = spatialSamplingInterval;
  }

  public float getPulseWidth() {
    return pulseWidth;
  }

  public void setPulseWidth(float pulseWidth) {
    this.pulseWidth = pulseWidth;
  }

  public int getStartLocusIndex() {
    return startLocusIndex;
  }

  public void setStartLocusIndex(int startLocusIndex) {
    this.startLocusIndex = startLocusIndex;
  }

  public int getNumberOfLoci() {
    return numberOfLoci;
  }

  public void setNumberOfLoci(int numberOfLoci) {
    this.numberOfLoci = numberOfLoci;
  }

  public int getSecondsToRun() {
    return secondsToRun;
  }

  public void setSecondsToRun(int secondsToRun) {
    this.secondsToRun = secondsToRun;
  }

}
