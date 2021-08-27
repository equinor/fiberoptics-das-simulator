package com.equinor.fiberoptics.das.producer.variants.simulatorboxunit;

import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.core.publisher.Flux;

import java.util.List;

@ActiveProfiles("test")
@SpringBootTest(classes=SimulatorBoxUnit.class)
@RunWith(SpringRunner.class)
public class SimulatorBoxUnitTest {

  @Autowired
  SimulatorBoxUnit simulatorBoxUnit;

  @Test
  public void testStreamFromSimulatorBox() {
    Flux<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>> output = simulatorBoxUnit.produce().get();
    List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>> results = output.collectList().block();
    for (PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> result: results) {
      System.out.println("-----------------------------------------");
      System.out.println("New DAS Measurement");

      for (Float floatValue: result.value.getAmplitudesFloat()) {
        System.out.println(floatValue);
      }
    }
  }
}
