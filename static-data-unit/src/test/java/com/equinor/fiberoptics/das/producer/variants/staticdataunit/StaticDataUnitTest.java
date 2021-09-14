package com.equinor.fiberoptics.das.producer.variants.staticdataunit;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

@ActiveProfiles("test")
@SpringBootTest(classes=StaticDataUnit.class)
@RunWith(SpringRunner.class)
public class StaticDataUnitTest {

  private static final Logger logger = LoggerFactory.getLogger(StaticDataUnitTest.class);
  private final static long millisInNano = 1_000_000;

  @Autowired
  StaticDataUnit staticDataUnit;

  @Test
  public void testStreamFromStaticDataBox() {
    AtomicInteger consumed = new AtomicInteger();

    Consumer<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> logOutput = value -> {
      for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> entry: value) {
        DASMeasurement measurement = entry.value;
        LocalDateTime ldt = Instant.ofEpochMilli(measurement.getStartSnapshotTimeNano() / millisInNano).atZone(ZoneId.systemDefault()).toLocalDateTime();

        logger.info("Locus {} with {} has {} amplitudes", measurement.getLocus(), ldt, measurement.getAmplitudesFloat().size());
        assertEquals("Number of amplitudes is as configured", 8192, measurement.getAmplitudesFloat().size());

        // Amplitudes are, for all loci and all amplitudes, 0 for first message, 1 for second ... n
        Float expected = (float) consumed.get();
        Float firstAmplitude = measurement.getAmplitudesFloat().get(0);

        assertEquals("Amplitude is expected", expected, firstAmplitude);
      }

      assertEquals("Number of loci is as configured", 3, value.size());
      consumed.getAndIncrement();
    };

    CountDownLatch latch = new CountDownLatch(1);
    staticDataUnit.produce()
      .subscribe(logOutput,
      (ex) -> {
        logger.info("Error emitted: " + ex.getMessage());
      },
      () -> {
        latch.countDown();
      });

    Helpers.wait(latch);

    assertEquals("Number consumed is as configured", 10, consumed.get());
  }
}
