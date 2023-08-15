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
import com.equinor.kafka.KafkaConfiguration;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;


@Component
public class DasProducerFactory {
  private static final Logger logger = LoggerFactory.getLogger(DasProducerFactory.class);

  private final HttpUtils _httpUtils;
  private final KafkaConfiguration _configuration;
  private final DasProducerConfiguration _dasProducerConfig;

  private final ApplicationContext _applicationContext;

  DasProducerFactory(KafkaConfiguration kafkaConfig, HttpUtils http, DasProducerConfiguration dasProducerConfig,
                     ApplicationContext applicationContext) {
    _configuration = kafkaConfig;
    _httpUtils = http;
    _dasProducerConfig = dasProducerConfig;
    _applicationContext = applicationContext;
  }

  @PreDestroy
  public void onDestroy() throws Exception {
    logger.info("Spring Container is destroyed!");
    Thread.sleep(1000);
  }

  @Bean
  public KafkaProducer<DASMeasurementKey, DASMeasurement> producerFactory() {

    logger.info("Preflight checking if initiator service is alive.");
    while (true) {
      String healthCheckSi = _dasProducerConfig.getInitiatorserviceUrl().trim() + "/actuator/health";
      if (_httpUtils.checkIfServiceIsFine(healthCheckSi)) break;
      //Just do it
      logger.info("Trying to reach Stream initiator service on: {} looking for a 200 OK.", healthCheckSi);
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        logger.warn("Unable to sleep");
      }
    }

    logger.info("Calling start acquisition on URL {}", _dasProducerConfig.getInitiatorserviceUrl().trim());
    AcquisitionStartDto acquisition = _httpUtils.startAcquisition();
    logger.info("Got TOPIC={}, BOOTSTRAP_SERVERS={}, SCHEMA_REGISTRY={}, NUMBER_OF_PARTITIONS={}",
      acquisition.getTopic(), acquisition.getBootstrapServers(), acquisition.getSchemaRegistryUrl(),
      acquisition.getNumberOfPartitions());
    if (acquisition.getNumberOfPartitions() <= 0) {
      logger.error("We are unable to run when the destination topic has {} partitions. Exiting.",
        acquisition.getNumberOfPartitions());
      logger.info("Stopping");
      int exitValue = SpringApplication.exit(_applicationContext);
      System.exit(exitValue);
    }

    String actualSchemaRegistryServers;
    if (_dasProducerConfig.getOverrideSchemaRegistryWith() != null
      && !_dasProducerConfig.getOverrideSchemaRegistryWith().isBlank()) {
      actualSchemaRegistryServers = _dasProducerConfig.getOverrideSchemaRegistryWith();
      logger.info("Overriding incoming schema registry server {} with: {}", acquisition.getSchemaRegistryUrl(),
        actualSchemaRegistryServers);
    } else {
      actualSchemaRegistryServers = acquisition.getSchemaRegistryUrl();
    }
    logger.info("Preflight checking if Schema registry is alive.");
    while (!_httpUtils.checkIfServiceIsFine(actualSchemaRegistryServers)) {
      //Just do it
      logger.info("Trying to reach {} looking for a 200 OK.", actualSchemaRegistryServers);
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        logger.warn("Unable to sleep");
      }
    }
    _dasProducerConfig.setPartitionAssignments(acquisition.getPartitionAssignments());
    _configuration.setTopic(acquisition.getTopic());
    _configuration.setPartitions(acquisition.getNumberOfPartitions());
    String actualBootstrapServeras;
    if (_dasProducerConfig.getOverrideBootstrapServersWith() != null
      && !_dasProducerConfig.getOverrideBootstrapServersWith().isBlank()) {
      actualBootstrapServeras = _dasProducerConfig.getOverrideBootstrapServersWith();
      logger.info("Overriding incoming bootstrap server {} with: {}", acquisition.getBootstrapServers(),
        actualBootstrapServeras);
    } else {
      actualBootstrapServeras = acquisition.getBootstrapServers();
    }

    return new KafkaProducer<>(_configuration.kafkaProperties(actualBootstrapServeras,
      actualSchemaRegistryServers));
  }
}
