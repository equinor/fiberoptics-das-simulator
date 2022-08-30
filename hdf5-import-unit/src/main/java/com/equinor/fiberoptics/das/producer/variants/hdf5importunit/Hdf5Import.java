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
package com.equinor.fiberoptics.das.producer.variants.hdf5importunit;

import com.equinor.fiberoptics.das.producer.variants.GenericDasProducer;
import com.equinor.fiberoptics.das.producer.variants.PackageStepCalculator;
import com.equinor.fiberoptics.das.producer.variants.PartitionKeyValueEntry;
import fiberoptics.time.message.v1.DASMeasurement;
import fiberoptics.time.message.v1.DASMeasurementKey;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Node;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * This is a hdf5 data unit for testing import of hdf5 files into the platform.
 *
 * @author Lindvar Lægran, llag@equinor.com
 */
@Component("Hdf5ImportUnit")
@EnableConfigurationProperties({Hdf5ImportConfiguration.class})
public class Hdf5Import implements GenericDasProducer {
  private static final Logger logger = LoggerFactory.getLogger(Hdf5Import.class);

  private final Hdf5ImportConfiguration _configuration;

  private List<Path> pathList;

  public Hdf5Import(Hdf5ImportConfiguration configuration) {
    this._configuration = configuration;
    pathList = getListOfHdf5Files(configuration.getFileDirectory());
  }

  @Override
  public Flux<List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>>> produce() {
    long delay = 0;
    long take = 0;
    if (_configuration.getNumberOfFiles() != null && _configuration.getNumberOfFiles() > 0) {
      take = _configuration.getNumberOfFiles().intValue();
    } else {
      take = pathList.size();
    }
    logger.info(String.format("Starting to produce %d data", take));


    return Flux
      .interval(Duration.ofMillis(delay))
      .take(take)
      .map(tick -> {
        float[][] hdf5_data = getHdf5DataFromFile(pathList.get(tick.intValue()));
        long[] hdf5_time = getHdf5TimeFromFile(pathList.get(tick.intValue()));
        List<PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement>> data = new ArrayList<>();
        int i = 0;
        while (true){
          int from = _configuration.getAmplitudesPrPackage() * i;
          int to = from + _configuration.getAmplitudesPrPackage();
          data.addAll(IntStream.range(0, _configuration.getNumberOfLoci())
            .mapToObj(currentLocus -> constructAvroObjects(currentLocus, getDataForLocus(hdf5_data[currentLocus], from, to), hdf5_time[from]))
            .collect(Collectors.toList()));
          if(to >= hdf5_data[0].length){
            break;
          }
          i++;
        }

        return data;
      });
  }



  private List<Float> getDataForLocus(float[] hdf5_data, int from, int to) {
      return Arrays.asList(ArrayUtils.toObject(Arrays.copyOfRange(hdf5_data, from, to)));
 }

  private List<Path> getListOfHdf5Files(String directory){
    try (Stream<Path> stream = Files.walk(Paths.get(directory))) {
      return stream.map(Path::normalize)
        .filter(Files::isRegularFile)
        .filter(path -> path.getFileName().toString().endsWith(".h5"))
        .collect(Collectors.toList());
    } catch (IOException e) {
      logger.error("Can not get hdf5 files from " + directory, e);
    }
    return new ArrayList<>();
  }

  private PartitionKeyValueEntry<DASMeasurementKey, DASMeasurement> constructAvroObjects(int currentLocus, List<Float> data, long dataTime) {
    if(data == null){
      return new PartitionKeyValueEntry<>(
        DASMeasurementKey.newBuilder()
          .setLocus(currentLocus)
          .build(),
        DASMeasurement.newBuilder()
          .setStartSnapshotTimeNano(dataTime)
          .setTrustedTimeSource(true)
          .setLocus(currentLocus)
          .setDataIsMissing(true)
          .build(),
        currentLocus);
    } else {
      return new PartitionKeyValueEntry<>(
        DASMeasurementKey.newBuilder()
          .setLocus(currentLocus)
          .build(),
        DASMeasurement.newBuilder()
          .setStartSnapshotTimeNano(dataTime)
          .setTrustedTimeSource(true)
          .setLocus(currentLocus)
          .setAmplitudesFloat(data)
          .build(),
        currentLocus);
    }

  }


  private float[][] getHdf5DataFromFile(Path hdf5File) {
    try (HdfFile hdfFile = new HdfFile(hdf5File)) {
      Dataset dataset = hdfFile.getDatasetByPath("Acquisition/Raw[0]/RawData");
      Object data = dataset.getData();
      return transpose((float[][]) data);
    }
  }

  private long[] getHdf5TimeFromFile(Path hdf5File) {
    try (HdfFile hdfFile = new HdfFile(hdf5File)) {
      Dataset dataset = hdfFile.getDatasetByPath("Acquisition/Raw[0]/RawDataTime");
      long[][] data = (long[][])dataset.getData();
      return transpose(data)[0];
    }
  }

  /*
  private Object getHdf5Attribute(Path hdf5File) {
    try (HdfFile hdfFile = new HdfFile(hdf5File)) {
      Attribute attribute = hdfFile.getAttribute("Acquisition");
      Object attributeData = attribute.getData();
      System.out.println(ArrayUtils.toString(attributeData)); //NOSONAR - sout in example
      return attribute;
    }
  }
*/
  public static float[][] transpose(float[][] matrix) {
    int rows = matrix.length;
    int columns = matrix[0].length;
    float transpose[][] = new float[columns][rows];

    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < columns; j++) {
        transpose[j][i] = matrix[i][j];
      }
    }
    return transpose;
  }

  public static long[][] transpose(long[][] matrix) {
    int rows = matrix.length;
    int columns = matrix[0].length;
    long transpose[][] = new long[columns][rows];

    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < columns; j++) {
        transpose[j][i] = matrix[i][j];
      }
    }
    return transpose;
  }



}
