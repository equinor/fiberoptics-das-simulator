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

import com.equinor.fiberoptics.das.DasProducerFactory;
import com.equinor.fiberoptics.das.producer.DasProducerConfiguration;
import com.equinor.fiberoptics.das.producer.variants.GenericDasProducer;
import com.equinor.kafka.KafkaRelay;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanFactory;
import reactor.core.publisher.Flux;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that non-remote mode still issues a stop signal to streaminitiator
 * when the producer completes or errors.
 */
public class NonRemoteStopSignalTest {

  @Test
  public void nonRemoteProducer_sendsStop_onCompletion() {
    BeanFactory beanFactory = mock(BeanFactory.class);
    DasProducerConfiguration cfg = new DasProducerConfiguration();
    KafkaRelay kafkaRelay = mock(KafkaRelay.class);
    DasProducerFactory dasProducerFactory = mock(DasProducerFactory.class);
    when(dasProducerFactory.getLastAcquisitionId()).thenReturn("acq-1");

    DasProducerApplication app = new DasProducerApplication(beanFactory, cfg, kafkaRelay, dasProducerFactory);
    GenericDasProducer producer = () -> Flux.empty();

    app.runNonRemoteProducer(producer, false);

    verify(dasProducerFactory).stopAcquisitionBestEffort("acq-1");
  }

  @Test
  public void nonRemoteProducer_sendsStop_onError() {
    BeanFactory beanFactory = mock(BeanFactory.class);
    DasProducerConfiguration cfg = new DasProducerConfiguration();
    KafkaRelay kafkaRelay = mock(KafkaRelay.class);
    DasProducerFactory dasProducerFactory = mock(DasProducerFactory.class);
    when(dasProducerFactory.getLastAcquisitionId()).thenReturn("acq-2");

    DasProducerApplication app = new DasProducerApplication(beanFactory, cfg, kafkaRelay, dasProducerFactory);
    GenericDasProducer producer = () -> Flux.error(new RuntimeException("boom"));

    app.runNonRemoteProducer(producer, false);

    verify(dasProducerFactory).stopAcquisitionBestEffort("acq-2");
  }
}
