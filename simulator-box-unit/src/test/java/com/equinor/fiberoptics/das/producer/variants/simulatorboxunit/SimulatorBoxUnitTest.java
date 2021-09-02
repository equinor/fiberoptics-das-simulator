package com.equinor.fiberoptics.das.producer.variants.simulatorboxunit;

import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import com.equinor.fiberoptics.das.producer.variants.util.Helpers;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

@ActiveProfiles("test")
@SpringBootTest(classes=SimulatorBoxUnit.class)
@RunWith(SpringRunner.class)
public class SimulatorBoxUnitTest {

  private static final Logger logger = LoggerFactory.getLogger(SimulatorBoxUnitTest.class);
  private final static long millisInNano = 1_000_000;

  @Autowired
  SimulatorBoxUnit simulatorBoxUnit;

  @Test
  public void testStreamFromSimulatorBox() {
    Consumer<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> logOutput = value -> {
      for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry: value) {
        DASMeasurement measurement = entry.value;
        LocalDateTime ldt = Instant.ofEpochMilli(measurement.getStartSnapshotTimeNano() / millisInNano).atZone(ZoneId.systemDefault()).toLocalDateTime();

        logger.info("Locus {} with {} has {} amplitudes", measurement.getLocus(), ldt, measurement.getAmplitudesFloat().size());
      }
    };

    CountDownLatch latch = new CountDownLatch(1);
    simulatorBoxUnit.produce()
      .subscribe(logOutput,
      (ex) -> {
        logger.info("Error emitted: " + ex.getMessage());
      },
      () -> {
        latch.countDown();
      });

    Helpers.wait(latch);
  }
}
