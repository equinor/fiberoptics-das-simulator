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

package com.equinor.fiberoptics.das.remotecontrol.profile;

import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.remotecontrol.RemoteControlService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.stereotype.Component;

/**
 * Resolves DAS simulator profiles from the configured profiles directory.
 */
@Component
public class DasSimulatorProfileResolver implements AcquisitionProfileResolver {

  private final DasProducerConfiguration _dasProducerConfiguration;
  private final ObjectMapper _objectMapper;

  public DasSimulatorProfileResolver(
      DasProducerConfiguration dasProducerConfiguration,
      ObjectMapper objectMapper) {
    _dasProducerConfiguration = dasProducerConfiguration;
    _objectMapper = objectMapper;
  }

  @Override
  public String resolveAcquisitionJson(JsonNode customNode) {
    if (customNode == null || customNode.isNull() || !customNode.isObject()) {
      throw new RemoteControlService.BadRequestException(
        "Custom must be an object and include das-simulator-profile."
      );
    }

    JsonNode profileNode = customNode.get("das-simulator-profile");
    if (profileNode == null || !profileNode.isTextual() || profileNode.asText().isBlank()) {
      throw new RemoteControlService.BadRequestException(
        "Custom.das-simulator-profile is required."
      );
    }

    String profileId = profileNode.asText().trim();
    if (profileId.contains("/") || profileId.contains("\\") || profileId.contains("..")) {
      throw new RemoteControlService.BadRequestException("Invalid das-simulator-profile value.");
    }

    String profilesDirectory;
    if (_dasProducerConfiguration.getRemoteControl() != null) {
      profilesDirectory = _dasProducerConfiguration.getRemoteControl()
        .getProfilesDirectory();
    } else {
      profilesDirectory = "remote-control-profiles";
    }
    if (profilesDirectory == null || profilesDirectory.isBlank()) {
      profilesDirectory = "remote-control-profiles";
    }

    Path baseDir = Path.of(profilesDirectory).normalize();
    Path profileFile = baseDir.resolve(profileId + ".json").normalize();
    if (!profileFile.startsWith(baseDir)) {
      throw new RemoteControlService.BadRequestException("Invalid das-simulator-profile value.");
    }

    if (!Files.exists(profileFile)) {
      throw new AcquisitionProfileNotFoundException("No profile file found for " + profileId + ".");
    }

    try {
      String json = Files.readString(profileFile, StandardCharsets.UTF_8);
      _objectMapper.readTree(json);
      return json;
    } catch (AcquisitionProfileNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
        "Profile file " + profileFile + " could not be read/parsed: " + e.getMessage(),
        e
      );
    }
  }
}

