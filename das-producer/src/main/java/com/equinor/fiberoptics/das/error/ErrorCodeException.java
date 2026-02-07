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

package com.equinor.fiberoptics.das.error;

import org.springframework.http.HttpStatus;

/**
 * Exception that carries a structured error code and HTTP status.
 */
public class ErrorCodeException extends RuntimeException {

  private final String _errorCode;
  private final HttpStatus _httpStatus;

  public ErrorCodeException(String errorCode, HttpStatus httpStatus, String message) {
    super(message);
    _errorCode = errorCode;
    _httpStatus = httpStatus;
  }

  public ErrorCodeException(String errorCode, HttpStatus httpStatus, String message, Throwable cause) {
    super(message, cause);
    _errorCode = errorCode;
    _httpStatus = httpStatus;
  }

  public String getErrorCode() {
    return _errorCode;
  }

  public HttpStatus getHttpStatus() {
    return _httpStatus;
  }
}
