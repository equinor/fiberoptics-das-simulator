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
package com.equinor.fiberoptics.switching.api;

import com.equinor.fiberoptics.switching.api.services.Switching;
import fiberoptics.config.acquisition.v2.DASAcquisition;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@Api(tags = {SwaggerConfiguration.SWITCHING_CONTROLLER_TAG})
@RestController
public class SwitchingController {
  private static final String BASE_PATH = "/api/v2.0";

  @Value("${api.key}")
  private String apiKey;

  private final Switching switching;

  @Autowired
  public SwitchingController(Switching switching) {
    this.switching = switching;
  }

  @PostMapping(BASE_PATH + "/acquisitions/start")
  @ApiOperation(
    value = "Will start a new acquisition, " +
      "or return error if one is already running"
  )
  void startAcquisition(
    @RequestHeader(name = "X-API-Key")String apiKey,
    @RequestBody DASAcquisition acquisition
  ) {
    if (apiKey.equals(this.apiKey)) {
      switching.start(acquisition);
    } else {
      throw new ResponseStatusException(
        HttpStatus.UNAUTHORIZED);
    }
  }

  @PostMapping(BASE_PATH + "/acquisitions/stop")
  @ApiOperation(
    value = "Will stop the acquisition, " +
      " or return error if it is not running"
  )
  void stopAcquisition(
    @RequestHeader(name = "X-API-Key")String apiKey,
    @RequestBody DASAcquisition acquisition
  ) {
    if (apiKey.equals(this.apiKey)) {
      switching.start(acquisition);
    } else {
      throw new ResponseStatusException(
        HttpStatus.UNAUTHORIZED);
    }
  }

  @PostMapping(BASE_PATH + "/acquisitions/switch")
  @ApiOperation(
    value = "Will stop any running acquisition, " +
      " and start the provided acquisition"
  )
  void switchTo(
    @RequestHeader(name = "X-API-Key")String apiKey,
    @RequestBody DASAcquisition acquisition
  ) {
    if (apiKey.equals(this.apiKey)) {
      switching.start(acquisition);
    } else {
      throw new ResponseStatusException(
        HttpStatus.UNAUTHORIZED);
    }
  }

}
