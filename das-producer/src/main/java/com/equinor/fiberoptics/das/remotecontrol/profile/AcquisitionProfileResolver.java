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

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Resolves acquisition profiles from incoming request metadata.
 */
public interface AcquisitionProfileResolver {
  /**
   * Resolves a profile JSON payload for an acquisition request.
   *
   * @param customNode The {@code Custom} part of an incoming APPLY request.
   * @return A full DASAcquisition JSON payload to use for starting the acquisition.
   */
  String resolveAcquisitionJson(JsonNode customNode);
}

