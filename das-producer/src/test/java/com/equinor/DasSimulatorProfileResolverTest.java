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

import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.remotecontrol.RemoteControlService;
import com.equinor.fiberoptics.das.remotecontrol.profile.AcquisitionProfileNotFoundException;
import com.equinor.fiberoptics.das.remotecontrol.profile.DasSimulatorProfileResolver;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link DasSimulatorProfileResolver}.
 *
 * <p>The resolver is responsible for turning the caller-provided Custom block
 * (specifically Custom.das-simulator-profile) into a full DAS acquisition JSON
 * loaded from disk. These tests focus on the contract:
 *
 * <ul>
 *   <li>Valid profile id resolves to the file contents.</li>
 *   <li>Missing file results in a not-found error.</li>
 *   <li>Missing profile id in Custom results in a bad request error.</li>
 * </ul>
 *
 * <p>We use {@link TempDir} to isolate file IO and avoid relying on repo assets
 * so the tests are deterministic and self-contained.</p>
 */
public class DasSimulatorProfileResolverTest {

  @Test
  public void resolveAcquisitionJson_returnsFileContents(@TempDir Path tempDir) throws Exception {
    // Arrange: write a profile file and point the resolver at the temp directory.
    String profileId = "019c0d85-dc19-7dfe-859e-8d0c066f3c46";
    String content = "{\"NumberOfLoci\":1,\"StartLocusIndex\":0,\"AcquisitionId\":\"a\",\"MeasurementStartTime\":\"b\"}";
    Files.writeString(tempDir.resolve(profileId + ".json"), content, StandardCharsets.UTF_8);

    DasProducerConfiguration cfg = new DasProducerConfiguration();
    cfg.getRemoteControl().setProfilesDirectory(tempDir.toString());
    ObjectMapper mapper = new ObjectMapper();
    DasSimulatorProfileResolver resolver = new DasSimulatorProfileResolver(cfg, mapper);

    // Act: resolve the profile from the Custom payload.
    JsonNode custom = mapper.readTree("{\"das-simulator-profile\":\"" + profileId + "\"}");
    assertEquals(content, resolver.resolveAcquisitionJson(custom));
  }

  @Test
  public void resolveAcquisitionJson_throwsNotFound_whenMissing(@TempDir Path tempDir) {
    // Arrange: point to an empty temp directory and request a missing profile id.
    DasProducerConfiguration cfg = new DasProducerConfiguration();
    cfg.getRemoteControl().setProfilesDirectory(tempDir.toString());
    ObjectMapper mapper = new ObjectMapper();
    DasSimulatorProfileResolver resolver = new DasSimulatorProfileResolver(cfg, mapper);

    JsonNode custom;
    try {
      custom = mapper.readTree("{\"das-simulator-profile\":\"does-not-exist\"}");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Act + Assert: missing file should surface as a dedicated not-found exception.
    assertThrows(AcquisitionProfileNotFoundException.class, () -> resolver.resolveAcquisitionJson(custom));
  }

  @Test
  public void resolveAcquisitionJson_throwsBadRequest_whenMissingProfileId(@TempDir Path tempDir) throws Exception {
    // Arrange: Custom payload lacks das-simulator-profile.
    DasProducerConfiguration cfg = new DasProducerConfiguration();
    cfg.getRemoteControl().setProfilesDirectory(tempDir.toString());
    ObjectMapper mapper = new ObjectMapper();
    DasSimulatorProfileResolver resolver = new DasSimulatorProfileResolver(cfg, mapper);

    // Act + Assert: request is malformed and should be rejected early.
    JsonNode custom = mapper.readTree("{\"other\":\"x\"}");
    assertThrows(RemoteControlService.BadRequestException.class, () -> resolver.resolveAcquisitionJson(custom));
  }
}
