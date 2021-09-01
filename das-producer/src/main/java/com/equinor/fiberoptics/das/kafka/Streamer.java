package com.equinor.fiberoptics.das.kafka;

import com.equinor.DasProducerApplication;
import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.PackageStepCalculator;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.RandomDataCache;
import com.equinor.fiberoptics.das.producer.variants.simulatorboxunit.SimulatorBoxUnitConfiguration;
import com.equinor.kafka.KafkaConfiguration;
import com.equinor.kafka.KafkaSender;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Component
public class Streamer {

  private final static long millisInNano = 1_000_000;
  private static final Logger logger = LoggerFactory.getLogger(Streamer.class);

  private final KafkaSender _kafkaSendChannel;
  private final KafkaConfiguration _kafkaConf;
  private final DasProducerConfiguration _dasProducerConfig;

  Streamer(
    KafkaConfiguration kafkaConfig,
    KafkaSender kafkaSendChannel,
    DasProducerConfiguration dasProducerConfiguration) {
    this._kafkaConf = kafkaConfig;
    this._kafkaSendChannel = kafkaSendChannel;
    this._dasProducerConfig = dasProducerConfiguration;
  }

  public void relayToKafka(PackageStepCalculator stepCalculator, PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> partitionEntry) {
    logger.info("Sending message on Kafka");
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
