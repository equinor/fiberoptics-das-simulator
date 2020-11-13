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
package com.equinor.fiberoptics.das.producer.dto;

import java.util.Map;

public class AcquisitionStartDto {
  private String topic;
  private String bootstrapServers;
  private String schemaRegistryUrl;
  private int numberOfPartitions;
  private Map<Integer, Integer> partitionAssignments;

  public AcquisitionStartDto() {
  }

  public AcquisitionStartDto(String topic, String bootstrapServers, String schemaRegistryUrl,
                      int numberOfPartitions, Map<Integer, Integer> partitionAssignments) {
    this.topic = topic;
    this.bootstrapServers = bootstrapServers;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.numberOfPartitions = numberOfPartitions;
    this.partitionAssignments = partitionAssignments;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  public void setSchemaRegistryUrl(String schemaRegistryUrl) {
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  public int getNumberOfPartitions() {
    return numberOfPartitions;
  }

  public void setNumberOfPartitions(int numberOfPartitions) {
    this.numberOfPartitions = numberOfPartitions;
  }

  public Map<Integer, Integer> getPartitionAssignments() {
    return partitionAssignments;
  }

  public void setPartitionAssignments(Map<Integer, Integer> partitionAssignments) {
    this.partitionAssignments = partitionAssignments;
  }
}
