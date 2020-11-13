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

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * Configuration class for the DAS random adapter that creates pseudo-random amplitude values for the DAS simulator.
 *
 * @author Espen Tjonneland, espen@tjonneland.no
 */
@ConfigurationProperties(prefix = "das.producer")
public class DasProducerConfiguration {

  private int amplitudesPrPackage;
  private String initiatorserviceUrl;
  private String initiatorserviceApiKey;
  private String kafkaTopicName;
  private Boolean running;
  private Boolean sending;
  private String overrideBootstrapServersWith;
  private String overrideSchemaRegistryWith;

  private Map<Integer, Integer> partitionAssignments;

  public Map<Integer, Integer> getPartitionAssignments() {
    return partitionAssignments;
  }

  public void setPartitionAssignments(Map<Integer, Integer> partitionAssignments) {
    this.partitionAssignments = partitionAssignments;
  }

  public String getOverrideBootstrapServersWith() {
    return overrideBootstrapServersWith;
  }

  public void setOverrideBootstrapServersWith(String overrideBootstrapServersWith) {
    this.overrideBootstrapServersWith = overrideBootstrapServersWith;
  }

  public String getOverrideSchemaRegistryWith() {
    return overrideSchemaRegistryWith;
  }

  public void setOverrideSchemaRegistryWith(String overrideSchemaRegistryWith) {
    this.overrideSchemaRegistryWith = overrideSchemaRegistryWith;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public void setKafkaTopicName(String kafkaTopicName) {
    this.kafkaTopicName = kafkaTopicName;
  }

  public Boolean isRunning() {
    return running;
  }

  public void setIsRunning(boolean should) {
    running = should;
  }

  public void signalRunning() {
    this.running = true;
  }

  public void signalStopped() {
    this.running = false;
  }

  public Boolean isSending() {
    return sending;
  }

  public void signalSending() {
    this.sending = true;
  }

  public void signalNotSending() {
    this.sending = false;
  }

  public String getInitiatorserviceUrl() {
    return initiatorserviceUrl;
  }

  public void setInitiatorserviceUrl(String initiatorserviceUrl) {
    this.initiatorserviceUrl = initiatorserviceUrl;
  }

  public String getInitiatorserviceApiKey() {
    return initiatorserviceApiKey;
  }

  public void setInitiatorserviceApiKey(String initiatorserviceApiKey) {
    this.initiatorserviceApiKey = initiatorserviceApiKey;
  }


  public int getAmplitudesPrPackage() {
    return amplitudesPrPackage;
  }

  public void setAmplitudesPrPackage(int amplitudesPrPackage) {
    this.amplitudesPrPackage = amplitudesPrPackage;
  }


}
