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
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

/**
 * HTTP client helper for acquisition start/stop calls.
 */
@Service
public class HttpUtils {
  private static final Logger _logger = LoggerFactory.getLogger(HttpUtils.class);
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);
  private final SimulatorBoxUnitConfiguration _simBoxConfig;
  private final DasProducerConfiguration _dasProducerConfig;
  private final RestTemplate _restTemplate;

  enum SchemaVersions {
    V1, V2
  }

  private static final String API_ENDPOINT = "/%s/acquisition/start";
  private static final String API_STOP_ENDPOINT = "/api/v1/acquisition/stop/%s";

  HttpUtils(
      SimulatorBoxUnitConfiguration simUnitConfig,
      DasProducerConfiguration dasProdConfig,
      RestTemplateBuilder restTemplateBuilder) {
    _simBoxConfig = simUnitConfig;
    _dasProducerConfig = dasProdConfig;
    _restTemplate = restTemplateBuilder
        .setConnectTimeout(CONNECT_TIMEOUT)
        .setReadTimeout(READ_TIMEOUT)
        .build();
  }

  /**
   * Starts an acquisition using the configured schema version.
   */
  public AcquisitionStartDto startAcquisition() {
    SchemaVersions version = SchemaVersions.valueOf(
        _dasProducerConfig.getAcquisitionStartVersion()
    );
    String json = version == SchemaVersions.V1 ? asV1Json() : asV2Json();
    return startAcquisition(json);
  }

  /**
   * Starts an acquisition using the provided JSON payload.
   */
  @SuppressWarnings("null")
  public AcquisitionStartDto startAcquisition(String acquisitionJson) {
    SchemaVersions version = SchemaVersions.valueOf(
        _dasProducerConfig.getAcquisitionStartVersion()
    );
    String apiEndpoint = version == SchemaVersions.V1
        ? String.format(API_ENDPOINT, "api")
        : String.format(API_ENDPOINT, "api/v2");

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("X-Api-Key", requiredInitiatorApiKey());

    var request = new HttpEntity<>(acquisitionJson, headers);
    var response = _restTemplate.exchange(
        _dasProducerConfig.getInitiatorserviceUrl() + apiEndpoint,
        HttpMethod.POST,
        request,
        AcquisitionStartDto.class
    );

    return response.getBody();
  }

  /**
   * Sends a stop request for the given acquisition id.
   */
  @SuppressWarnings("null")
  public void stopAcquisition(String acquisitionId) {
    if (acquisitionId == null || acquisitionId.isBlank()) {
      return;
    }

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("X-Api-Key", requiredInitiatorApiKey());

    String endpoint = String.format(API_STOP_ENDPOINT, acquisitionId);
    var request = new HttpEntity<>(null, headers);
    _restTemplate.exchange(
        _dasProducerConfig.getInitiatorserviceUrl() + endpoint,
        HttpMethod.POST,
        request,
        Void.class
    );
  }

  /**
   * Checks whether the given service responds with a 2xx status.
   */
  @SuppressWarnings("null")
  public boolean checkIfServiceIsFine(String service) {
    HttpStatusCode statusCode;
    try {
      statusCode = _restTemplate.getRequestFactory()
          .createRequest(new URI(service), HttpMethod.GET)
          .execute()
          .getStatusCode();
    } catch (Exception e) {
      _logger.info(
          "Got an exception when querying {}. Got: {}",
          service,
          e.getMessage()
      );
      return false;
    }
    _logger.info("Got status code {}", statusCode);
    return statusCode.is2xxSuccessful();
  }

  private String requiredInitiatorApiKey() {
    String apiKey = _dasProducerConfig.getInitiatorserviceApiKey();
    if (apiKey == null || apiKey.isBlank()) {
      throw new IllegalStateException(
          "INITIATOR_API_KEY (das.producer.initiatorservice-api-key) must be set."
      );
    }
    return apiKey.trim();
  }

  /**
   * Builds a version 1 acquisition JSON payload using simulator configuration.
   */
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
      (float) _simBoxConfig.getPulseRate() / 2,
      (float) _simBoxConfig.getPulseRate(),
      _simBoxConfig.getPulseWidth(),
      fiberoptics.config.acquisition.v1.Units.ns,
      Vendors.valueOf(_dasProducerConfig.getVendorCode()),
      custom,
      _simBoxConfig.getOpticalPathUUID(),
      _simBoxConfig.getBoxUUID(),
      acquisitionId
    ));
  }

  /**
   * Builds a version 2 acquisition JSON payload using simulator configuration.
   */
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
      (float) _simBoxConfig.getPulseRate() / 2,
      (float) _simBoxConfig.getPulseRate(),
      _simBoxConfig.getPulseWidth(),
      fiberoptics.config.acquisition.v2.Units.ns,
      _dasProducerConfig.getVendorCode(),
      custom,
      _simBoxConfig.getOpticalPathUUID(),
      _simBoxConfig.getBoxUUID(),
      acquisitionId
    ));
  }

}
