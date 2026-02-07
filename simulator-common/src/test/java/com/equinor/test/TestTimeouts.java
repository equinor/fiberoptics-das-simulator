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

package com.equinor.test;

import java.time.Duration;
import java.util.Locale;

/**
 * Test timeout helpers with environment/property overrides.
 */
public final class TestTimeouts {

  private static final double MULTIPLIER = readDouble("TEST_TIMEOUT_MULTIPLIER", 1.0);

  private TestTimeouts() {
  }

  public static Duration duration(String key, Duration defaultValue) {
    String value = readValue(key);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    return parseDuration(value.trim(), defaultValue);
  }

  public static Duration scaled(Duration defaultValue) {
    if (MULTIPLIER <= 0) {
      return defaultValue;
    }
    long millis = Math.round(defaultValue.toMillis() * MULTIPLIER);
    return Duration.ofMillis(Math.max(1L, millis));
  }

  public static long seconds(String key, long defaultValue) {
    String value = readValue(key);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    return Long.parseLong(value.trim());
  }

  private static String readValue(String key) {
    String value = System.getProperty(key);
    if (value != null) {
      return value;
    }
    return System.getenv(key);
  }

  private static double readDouble(String key, double defaultValue) {
    String value = readValue(key);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      return Double.parseDouble(value.trim());
    } catch (NumberFormatException ex) {
      return defaultValue;
    }
  }

  private static Duration parseDuration(String value, Duration fallback) {
    String lower = value.toLowerCase(Locale.ROOT);
    try {
      if (lower.matches("\\d+")) {
        return Duration.ofSeconds(Long.parseLong(lower));
      }
      if (lower.endsWith("ms")) {
        return Duration.ofMillis(Long.parseLong(lower.substring(0, lower.length() - 2)));
      }
      if (lower.endsWith("s")) {
        return Duration.ofSeconds(Long.parseLong(lower.substring(0, lower.length() - 1)));
      }
      if (lower.endsWith("m")) {
        return Duration.ofMinutes(Long.parseLong(lower.substring(0, lower.length() - 1)));
      }
      if (lower.endsWith("h")) {
        return Duration.ofHours(Long.parseLong(lower.substring(0, lower.length() - 1)));
      }
      return Duration.parse(value);
    } catch (Exception ex) {
      return fallback;
    }
  }
}
