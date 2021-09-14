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
package com.equinor;

import com.equinor.kafka.KafkaRelay;
import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.GenericDasProducer;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnitConfiguration;
import com.equinor.fiberoptics.das.producer.variants.util.Helpers;
import com.equinor.kafka.KafkaConfiguration;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.retry.annotation.EnableRetry;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * This is the entry point of the application.
 * It uses component scanning to find beans and configuration
 * in order to run the simulator.
 *
 * @author Espen Tjonneland, espen@tjonneland.no
 */
@SpringBootApplication
@EnableConfigurationProperties(
  {DasProducerConfiguration.class, SimulatorBoxUnitConfiguration.class, KafkaConfiguration.class}
  )
@EnableRetry
public class DasProducerApplication {

  private static final Logger logger = LoggerFactory.getLogger(DasProducerApplication.class);
  private final BeanFactory _beanFactory;
  private final DasProducerConfiguration _dasProducerConfig;
  private final KafkaRelay _kafkaRelay;

  @Autowired
  public DasProducerApplication(BeanFactory beanFactory, DasProducerConfiguration dasProducerConfig, KafkaRelay kafkaRelay) {
    this._beanFactory = beanFactory;
    this._dasProducerConfig = dasProducerConfig;
    this._kafkaRelay = kafkaRelay;
  }

  public static void main(final String[] args) {
    SpringApplication.run(DasProducerApplication.class, args);
  }

  @EventListener
  public void onApplicationEvent(ApplicationReadyEvent event) {
    logger.info("ApplicationReadyEvent");

    GenericDasProducer simulatorBoxUnit = _beanFactory.getBean(_dasProducerConfig.getVariant(), GenericDasProducer.class);
    Consumer<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> relayToKafka = value -> {
      for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry: value) {
        _kafkaRelay.relayToKafka(entry);
      }
    };

    CountDownLatch latch = new CountDownLatch(1);
    simulatorBoxUnit.produce()
      .subscribe(relayToKafka,
        (ex) -> {
          logger.info("Error emitted: " + ex.getMessage());
          ex.printStackTrace();
        },
        () -> {
          latch.countDown();
        });

    Helpers.wait(latch);
    _kafkaRelay.teardown();
    logger.info("Job done. Exiting.");
    System.exit(0);
  }
}
