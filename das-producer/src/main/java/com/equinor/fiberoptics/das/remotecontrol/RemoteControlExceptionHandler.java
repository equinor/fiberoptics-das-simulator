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

package com.equinor.fiberoptics.das.remotecontrol;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps remote-control errors to structured responses.
 */
@RestControllerAdvice(basePackages = "com.equinor.fiberoptics.das.remotecontrol")
public class RemoteControlExceptionHandler {

  @ExceptionHandler(RemoteControlService.BadRequestException.class)
  public ResponseEntity<ErrorResponse> handleBadRequest(
      RemoteControlService.BadRequestException ex,
      HttpServletRequest request) {
    return build(HttpStatus.BAD_REQUEST, "RC-400", ex.getMessage(), request);
  }

  @ExceptionHandler(RemoteControlService.UnauthorizedException.class)
  public ResponseEntity<ErrorResponse> handleUnauthorized(
      RemoteControlService.UnauthorizedException ex,
      HttpServletRequest request) {
    return build(HttpStatus.UNAUTHORIZED, "RC-401", "Unauthorized", request);
  }

  @ExceptionHandler(RemoteControlService.NotFoundException.class)
  public ResponseEntity<ErrorResponse> handleNotFound(
      RemoteControlService.NotFoundException ex,
      HttpServletRequest request) {
    return build(HttpStatus.NOT_FOUND, "RC-404", ex.getMessage(), request);
  }

  @ExceptionHandler(IllegalStateException.class)
  public ResponseEntity<ErrorResponse> handleIllegalState(
      IllegalStateException ex,
      HttpServletRequest request) {
    return build(HttpStatus.INTERNAL_SERVER_ERROR, "RC-500", ex.getMessage(), request);
  }

  private ResponseEntity<ErrorResponse> build(
      HttpStatus status,
      String code,
      String message,
      HttpServletRequest request) {
    String path = request == null ? null : request.getRequestURI();
    String safeMessage = message == null || message.isBlank() ? status.getReasonPhrase() : message;
    return ResponseEntity.status(status).body(ErrorResponse.of(code, safeMessage, path));
  }
}
