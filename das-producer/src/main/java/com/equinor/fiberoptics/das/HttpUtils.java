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
package com.equinor.fiberoptics.das;

import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.dto.AcquisitionStartDto;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnitConfiguration;
import com.google.gson.Gson;
import fiberoptics.config.acquisition.v1.Vendors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class HttpUtils {
  private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);
  private final SimulatorBoxUnitConfiguration _simBoxConfig;
  private final DasProducerConfiguration _dasProducerConfig;

  enum SchemaVersions {
    V1, V2
  }

  private static final String API_ENDPOINT = "/%s/acquisition/start";


  HttpUtils(SimulatorBoxUnitConfiguration simUnitConfig, DasProducerConfiguration dasProdConfig) {
    _simBoxConfig = simUnitConfig;
    _dasProducerConfig = dasProdConfig;
  }

  public AcquisitionStartDto startAcquisition() {
    SchemaVersions version = SchemaVersions.valueOf(_dasProducerConfig.getAcquisitionStartVersion());
    String apiEndpoint = version == SchemaVersions.V1 ? String.format(API_ENDPOINT, "api") : String.format(API_ENDPOINT, "api/v2");

    RestTemplate restTemplate = new RestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("X-Api-Key", _dasProducerConfig.getInitiatorserviceApiKey().trim());

    String json = version == SchemaVersions.V1?
      asV1Json() :
      asV2Json();

    var request = new HttpEntity<>(json, headers);
    var response = restTemplate.exchange(
      _dasProducerConfig.getInitiatorserviceUrl() + apiEndpoint,
      HttpMethod.POST,
      request,
      AcquisitionStartDto.class);

    return response.getBody();
  }

  public boolean checkIfServiceIsFine(String service) {
    RestTemplate rt = new RestTemplate();
    HttpStatusCode statusCode;
    try {
      statusCode = rt.getRequestFactory().createRequest(new URI(service), HttpMethod.GET)
        .execute().getStatusCode();
    } catch (Exception e) {
      logger.info("Got an exception when querying {}. Got: {}", service, e.getMessage());
      return false;
    }
    logger.info("Got status code {}", statusCode);
    return statusCode.is2xxSuccessful();
  }

  public String asV1Json() {
    String acquisitionId = UUID.randomUUID().toString();
    Map<String, String> custom = new HashMap<>();
    custom.put("AcquisitionStartCsu", "0");

    Gson gson = new Gson();
    return gson.toJson(new fiberoptics.config.acquisition.v1.DASAcquisition(
      "",
      _simBoxConfig.getStartTimeInstant().toString(),
      _simBoxConfig.getGaugeLength(),
      _simBoxConfig.getSpatialSamplingInterval(),
      _simBoxConfig.getNumberOfLoci(),
      _simBoxConfig.getStartLocusIndex(),

      (float)_simBoxConfig.getPulseRate()/2,
      (float)_simBoxConfig.getPulseRate(),
      _simBoxConfig.getPulseWidth(),
      fiberoptics.config.acquisition.v1.Units.ns,
      Vendors.valueOf(_dasProducerConfig.getVendorCode()),
      custom,
      _simBoxConfig.getOpticalPathUUID(),
      _simBoxConfig.getBoxUUID(),
      acquisitionId));
  }

  public String asV2Json() {
    String acquisitionId = UUID.randomUUID().toString();
    Map<String, String> custom = new HashMap<>();
    custom.put("AcquisitionStartCsu", "0");

    Gson gson = new Gson();
    return gson.toJson(new fiberoptics.config.acquisition.v2.DASAcquisition(
      "",
      _simBoxConfig.getStartTimeInstant().toString(),
      _simBoxConfig.getGaugeLength(),
      _simBoxConfig.getSpatialSamplingInterval(),
      _simBoxConfig.getNumberOfLoci(),
      _simBoxConfig.getStartLocusIndex(),
      (float)_simBoxConfig.getPulseRate()/2,
      (float)_simBoxConfig.getPulseRate(),
      _simBoxConfig.getPulseWidth(),
      fiberoptics.config.acquisition.v2.Units.ns,
      _dasProducerConfig.getVendorCode(),
      custom,
      _simBoxConfig.getOpticalPathUUID(),
      _simBoxConfig.getBoxUUID(),
      acquisitionId));
  }

}
