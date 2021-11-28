/*-
 * ========================LICENSE_START=================================
 * switching-api
 * %%
 * Copyright (C) 2020 - 2021 Equinor ASA
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
package com.equinor.fiberoptics.switching.api.services;

import fiberoptics.config.acquisition.v2.DASAcquisition;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Service
public class Switching {
  private static final Logger logger = LoggerFactory.getLogger(Switching.class);

  private DASAcquisition runningAcquisition;
  private InstrumentBoxConfiguration instrumentBoxConfiguration;

  public Switching(InstrumentBoxConfiguration instrumentBoxConfiguration) {
    this.instrumentBoxConfiguration = instrumentBoxConfiguration;
  }

  public void switchTo(DASAcquisition acquisition) {
    InstrumentBox instrumentBox = this.instrumentBoxConfiguration.getInstrumentBoxes().stream()
      .filter(box -> acquisition.getDasInstrumentBoxUUID().equals(box.uuid))
      .findAny()
      .orElse(null);

    if (instrumentBox == null) {
      throw new RuntimeException("Instrument box not found");
    }

    FiberOpticPath fiberOpticPath = instrumentBox.paths.stream()
      .filter(path -> acquisition.getOpticalPathUUID().equals(path.uuid))
      .findAny()
      .orElse(null);

    if (fiberOpticPath == null) {
      throw new RuntimeException("Path not found");
    }

    if (runningAcquisition != null) {
      logger.info("Stop " + acquisition.getAcquisitionId());
    }

    logger.info("Start " + acquisition.getAcquisitionId());
    runningAcquisition = acquisition;
  }

}

@ConfigurationProperties("das.boxes")
@Configuration
class InstrumentBoxConfiguration {
  private List<InstrumentBox> instrumentBoxes;

  public List<InstrumentBox> getInstrumentBoxes() {
    return instrumentBoxes;
  }

  public void setInstrumentBoxes(List<InstrumentBox> instrumentBoxes) {
    this.instrumentBoxes = instrumentBoxes;
  }
}

class InstrumentBox {
  public String uuid;
  public List<FiberOpticPath> paths;

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public List<FiberOpticPath> getPaths() {
    return paths;
  }

  public void setPaths(List<FiberOpticPath> paths) {
    this.paths = paths;
  }
}

class FiberOpticPath {
  public String uuid;

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }
}
