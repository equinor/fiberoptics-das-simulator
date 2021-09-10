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
package com.equinor.fiberoptics.das.producer.dto;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class AcquisitionStartRequest {

  @JsonProperty("SchemaVersion")
  private String schemaVersion = "2.0";
  @JsonProperty("AcquisitionId")
  private String aquisitionId;
  @JsonProperty("OpticalPathUUID")
  private String opticalPath;
  @JsonProperty("PulseWidth")
  private float pulseWidth;
  @JsonProperty("PulseWidthUnit")
  private String pulseWidthUnit = "ns";
  @JsonProperty("FacilityId")
  private String facilityId = "";
  @JsonProperty("MinimumFrequency")
  private float minimumFrequency;
  @JsonProperty("MaximumFrequency")
  private float maximumFrequency;
  @JsonProperty("DasInstrumentBoxUUID")
  private String dasInstrumentBox;
  @JsonProperty("SpatialSamplingInterval")
  private float spatialSamplingInterval;
  @JsonProperty("SpatialSamplingIntervalUnit")
  private String spatialSamplingIntervalUnit = "m";
  @JsonProperty("GaugeLength")
  private float gaugeLength;
  @JsonProperty("GaugeLengthUnit")
  private String gaugeLengthUnit = "m";
  @JsonProperty("TriggeredMeasurement")
  private boolean triggeredMeasurement = false;
  @JsonProperty("NumberOfLoci")
  private int numberOfLoci;
  @JsonProperty("PulseRate")
  private float pulseRate;
  @JsonProperty("MeasurementStartTime")
  private String measurementStartTime;
  @JsonProperty("VendorCode")
  private String vendorCode;
  @JsonProperty("StartLocusIndex")
  private int startLocusIndex;
  @JsonProperty("Custom")
  private Map<String, String> custom = new HashMap<>();

  public AcquisitionStartRequest(String acquisitionId, String dasInstrumentBoxUUID, String opticalPathUUID, String vendorCode,
                                 float gaugeLength, float spatialSamplingInterval,
                                 float pulseRate, float pulseWidth,
                                 int numberOfLoci, int startLocusIndex,
                                 float minFreq, String measurementStartTime) {

    this.setAquisitionId(acquisitionId);
    this.setOpticalPath(opticalPathUUID);
    this.setDasInstrumentBox(dasInstrumentBoxUUID);
    this.setVendorCode(vendorCode);
    this.setNumberOfLoci(numberOfLoci);
    this.setPulseRate(pulseRate);
    this.setGaugeLength(gaugeLength);
    this.setSpatialSamplingInterval(spatialSamplingInterval);
    this.setMaximumFrequency(pulseRate/2);
    this.setPulseWidth(pulseWidth);
    this.setStartLocusIndex(startLocusIndex);
    this.setMeasurementStartTime(measurementStartTime);
    this.setMinimumFrequency(minFreq);
    Custom custom = new Custom();
    custom.setDetails("AcquisitionStartCsu", "0");
  }

  @JsonAnySetter
  public void setCustom(String key, String value) {
      custom.put(key, value);
  }

  public class Custom {
    Map<String, String> details = new HashMap<>();

    @JsonAnySetter
    public void setDetails(String key, String value) {
      details.put(key, value);
    }

    public Map<String, String> getDetails() {
      return details;
    }
  }
}
