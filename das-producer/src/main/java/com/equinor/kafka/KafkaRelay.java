/*-
 * ========================LICENSE_START=================================
 * fiberoptics-das-simulator
 * %%
 * Copyright (C) 2020 - 2021 Equinor ASA
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
package com.equinor.kafka;

import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.PackageStepCalculator;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaRelay {

  private final static long millisInNano = 1_000_000;
  private static final Logger logger = LoggerFactory.getLogger(KafkaRelay.class);

  private final KafkaSender _kafkaSendChannel;
  private final KafkaConfiguration _kafkaConf;
  private final DasProducerConfiguration _dasProducerConfig;

  KafkaRelay(
    KafkaConfiguration kafkaConfig,
    KafkaSender kafkaSendChannel,
    DasProducerConfiguration dasProducerConfiguration) {
    this._kafkaConf = kafkaConfig;
    this._kafkaSendChannel = kafkaSendChannel;
    this._dasProducerConfig = dasProducerConfiguration;
  }

  public void relayToKafka(PackageStepCalculator stepCalculator, PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> partitionEntry) {
    int currentPartition = _dasProducerConfig.getPartitionAssignments().get(partitionEntry.value.getLocus()); //Use the one from stream initiator (Simulator mode)
    ProducerRecord<DASMeasurementKey, DASMeasurement> data =
      new ProducerRecord(_kafkaConf.getTopic(),
        currentPartition,
        stepCalculator.currentEpochMillis(),
        partitionEntry.key, partitionEntry.value);
    _kafkaSendChannel.send(data);
  }

  public void teardown() {
    logger.info("Send complete. Shutting down thread now");
    _kafkaSendChannel.close();
  }

}
