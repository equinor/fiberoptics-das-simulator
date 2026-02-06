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

package com.equinor.fiberoptics.das.remotecontrol;

import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import java.util.Optional;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/api/acquisition")
@ConditionalOnProperty(
    prefix = "das.producer.remote-control",
    name = "enabled",
    havingValue = "true"
)
public class RemoteControlController {

  private final RemoteControlService _remoteControlService;
  private final DasProducerConfiguration _dasProducerConfiguration;

  public RemoteControlController(
      RemoteControlService remoteControlService,
      DasProducerConfiguration dasProducerConfiguration) {
    _remoteControlService = remoteControlService;
    _dasProducerConfiguration = dasProducerConfiguration;
    ensureApiKeyConfigured();
  }

  @PostMapping(path = "/apply", consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> apply(
      @RequestHeader(value = "X-Api-Key", required = false) String apiKey,
      @RequestBody String acquisitionJson) {
    verifyApiKey(apiKey);
    _remoteControlService.apply(acquisitionJson);
    return ResponseEntity.ok().build();
  }

  @PostMapping(path = "/stop", consumes = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<?> stop(
      @RequestHeader(value = "X-Api-Key", required = false) String apiKey,
      @RequestBody(required = false) String acquisitionJson) {
    verifyApiKey(apiKey);
    RemoteControlService.StopResult result = _remoteControlService.stop(
        Optional.ofNullable(acquisitionJson)
    );
    return result == RemoteControlService.StopResult.NOT_FOUND
      ? ResponseEntity.notFound().build()
      : ResponseEntity.ok().build();
  }

  private void verifyApiKey(String apiKey) {
    if (_dasProducerConfiguration.getRemoteControl() == null) {
      return;
    }
    String configured = _dasProducerConfiguration.getRemoteControl().getApiKey();
    if (apiKey == null || !configured.equals(apiKey)) {
      throw new RemoteControlService.UnauthorizedException();
    }
  }

  private void ensureApiKeyConfigured() {
    if (_dasProducerConfiguration.getRemoteControl() == null) {
      return;
    }
    if (!_dasProducerConfiguration.getRemoteControl().isEnabled()) {
      return;
    }
    String configured = _dasProducerConfiguration.getRemoteControl().getApiKey();
    if (configured == null || configured.isBlank()) {
      throw new IllegalStateException(
        "Remote control is enabled but no API key is configured. "
          + "Set REMOTE_CONTROL_API_KEY (das.producer.remote-control.api-key)."
      );
    }
  }
}
