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
package com.equinor.fiberoptics.das.producer.variants;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionKeyValueEntryTest {

  @Test
  public void returnsKeyValuePartition() {
    PartitionKeyValueEntry<String, Integer> entry = new PartitionKeyValueEntry<>("k", 42, 3);

    assertEquals("k", entry.getKey());
    assertEquals(42, entry.getValue());
    assertEquals(3, entry.getPartition());
  }
}
