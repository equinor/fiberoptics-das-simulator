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
import com.equinor.fiberoptics.das.producer.dto.AcquisitionStartRequest;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnitConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;

@Service
public class HttpUtils {
  private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);
  private final SimulatorBoxUnitConfiguration _simBoxConfig;
  private final DasProducerConfiguration _dasProducerConfig;

  HttpUtils(SimulatorBoxUnitConfiguration simUnitConfig, DasProducerConfiguration dasProdConfig) {
    _simBoxConfig = simUnitConfig;
    _dasProducerConfig = dasProdConfig;
  }

  public AcquisitionStartDto startAcquisition() {
    RestTemplate restTemplate = new RestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("X-Api-Key", _dasProducerConfig.getInitiatorserviceApiKey().trim());
    String acquisitionId = UUID.randomUUID().toString();

    var request = new HttpEntity<>(new AcquisitionStartRequest(acquisitionId,
      _simBoxConfig.getBoxUUID(),
      _simBoxConfig.getOpticalPathUUID(),
      _simBoxConfig.getGaugeLength(),
      _simBoxConfig.getSpatialSamplingInterval(),
      _simBoxConfig.getPulseRate(),
      _simBoxConfig.getPulseWidth(),
      _simBoxConfig.getNumberOfLoci(),
      _simBoxConfig.getStartLocusIndex(),
      _simBoxConfig.getMinFreq(),
      Instant.now().toString()), headers);

    var response = restTemplate.exchange(
      _dasProducerConfig.getInitiatorserviceUrl() + "/api/acquisition/start",
      HttpMethod.POST,
      request,
      AcquisitionStartDto.class);

    return response.getBody();
  }

  public boolean checkIfServiceIsFine(String service) {
    RestTemplate rt = new RestTemplate();
    HttpStatus statusCode;
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

}
