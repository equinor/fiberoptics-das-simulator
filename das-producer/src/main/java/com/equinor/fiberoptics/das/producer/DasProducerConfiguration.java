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

package com.equinor.fiberoptics.das.producer;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration class for the DAS random adapter that creates pseudo-random amplitude values
 * for the DAS simulator.
 *
 * @author Espen Tjonneland, espen@tjonneland.no
 */
@ConfigurationProperties(prefix = "das.producer")
@Getter
@Setter
public class DasProducerConfiguration {

  private String vendorCode;
  private int amplitudesPrPackage;
  private String initiatorserviceUrl;
  private String acquisitionStartVersion;
  private String initiatorserviceApiKey;
  private String kafkaTopicName;
  private String overrideBootstrapServersWith;
  private String overrideSchemaRegistryWith;
  private String variant;

  private Map<Integer, Integer> partitionAssignments;

  private RemoteControl remoteControl = new RemoteControl();

  @Getter
  @Setter
  public static class RemoteControl {
    /**
     * When enabled, the simulator will not start producing data at startup.
     * Instead, it will wait for a remote APPLY call to start an acquisition.
     */
    private boolean enabled = false;

    /**
     * Required API key for the simulator's remote-control endpoints.
     * Callers must supply {@code X-Api-Key} with the same value.
     */
    private String apiKey;

    /**
     * Directory containing remote-control profiles as JSON files named
     * {@code <das-simulator-profile>.json}.
     */
    private String profilesDirectory = "remote-control-profiles";
  }
}
