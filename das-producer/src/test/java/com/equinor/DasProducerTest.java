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
import com.equinor.fiberoptics.das.remotecontrol.RemoteControlController;
import com.equinor.fiberoptics.das.remotecontrol.RemoteControlExceptionHandler;
import com.equinor.fiberoptics.das.remotecontrol.RemoteControlService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Controller-level tests for the remote-control endpoints.
 *
 * <p>These tests use {@link MockMvc} with a standalone controller to verify HTTP behavior
 * without starting the full Spring context. The focus is on request/response semantics:
 *
 * <ul>
 *   <li>API key is required when remote control is enabled.</li>
 *   <li>Valid API key allows APPLY and STOP to reach the service.</li>
 *   <li>STOP endpoint returns 404 when the acquisition is unknown.</li>
 * </ul>
 *
 * <p>This gives fast feedback and ensures the public API contract is stable.</p>
 */
public class DasProducerTest {

  private static final String API_KEY = "test-api-key";

  MockMvc mvc;
  RemoteControlService remoteControlService;

  @BeforeEach
  public void setUp() {
    // Build a controller with an enabled remote-control config and a known API key.
    remoteControlService = mock(RemoteControlService.class);
    DasProducerConfiguration cfg = new DasProducerConfiguration();
    cfg.getRemoteControl().setEnabled(true);
    cfg.getRemoteControl().setApiKey(API_KEY);
    RemoteControlController controller = new RemoteControlController(remoteControlService, cfg);
    mvc = MockMvcBuilders.standaloneSetup(controller)
      .setControllerAdvice(new RemoteControlExceptionHandler())
      .build();
  }

  @Test
  public void apply_requiresApiKey_whenConfigured() throws Exception {
    // No X-Api-Key header -> should be rejected.
    mvc.perform(
        post("/api/acquisition/apply")
          .contentType(MediaType.APPLICATION_JSON)
          .content("{}"))
      .andExpect(status().isUnauthorized());
  }

  @Test
  public void apply_ok_withApiKey() throws Exception {
    // Valid X-Api-Key -> APPLY should be accepted and delegated to service.
    mvc.perform(
        post("/api/acquisition/apply")
          .header("X-Api-Key", API_KEY)
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"NumberOfLoci\":100,\"StartLocusIndex\":0}"))
      .andExpect(status().isOk());

    verify(remoteControlService).apply(anyString());
  }

  @Test
  public void stop_returns404_whenUnknownAcquisition() throws Exception {
    // When service reports NOT_FOUND, the controller should surface HTTP 404.
    when(remoteControlService.stop(any())).thenReturn(RemoteControlService.StopResult.NOT_FOUND);

    mvc.perform(
        post("/api/acquisition/stop")
          .header("X-Api-Key", API_KEY)
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"AcquisitionId\":\"unknown\"}"))
      .andExpect(status().isNotFound());
  }

  @Test
  public void stop_ok_whenAlreadyStopped() throws Exception {
    // Already stopped is not an error; should return 200 OK.
    when(remoteControlService.stop(any())).thenReturn(RemoteControlService.StopResult.ALREADY_STOPPED);

    mvc.perform(
        post("/api/acquisition/stop")
          .header("X-Api-Key", API_KEY)
          .contentType(MediaType.APPLICATION_JSON)
          .content("{\"AcquisitionId\":\"1666612d-fb5e-11f0-bc8b-de1062e2899a\"}"))
      .andExpect(status().isOk());
  }
}
