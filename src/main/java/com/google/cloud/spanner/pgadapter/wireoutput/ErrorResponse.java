// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spanner.pgadapter.wireoutput;

import java.io.DataOutputStream;
import java.io.IOException;
import java.text.MessageFormat;

/**
 * Sends error information back to client.
 */
public class ErrorResponse extends WireOutput {

  private static final byte[] SEVERITY = "ERROR".getBytes(UTF8);
  private static final byte[] SQL_STATE = "XX000".getBytes(UTF8);
  private static final int HEADER_LENGTH = 4;
  private static final int FIELD_IDENTIFIER_LENGTH = 1;
  private static final int NULL_TERMINATOR_LENGTH = 1;

  private static final byte CODE_FLAG = 'C';
  private static final byte MESSAGE_FLAG = 'M';
  private static final byte SEVERITY_FLAG = 'S';
  private static final byte NULL_TERMINATOR = 0;


  private final byte[] errorMessage;

  public ErrorResponse(DataOutputStream output, Exception e) {
    super(output,
        HEADER_LENGTH
            + FIELD_IDENTIFIER_LENGTH + SEVERITY.length + NULL_TERMINATOR_LENGTH
            + FIELD_IDENTIFIER_LENGTH + SQL_STATE.length + NULL_TERMINATOR_LENGTH
            + FIELD_IDENTIFIER_LENGTH + getMessageFromException(e).length
            + NULL_TERMINATOR_LENGTH + NULL_TERMINATOR_LENGTH
    );
    this.errorMessage = getMessageFromException(e);
  }

  @Override
  protected void sendPayload() throws IOException {
    this.outputStream.writeByte(SEVERITY_FLAG);
    this.outputStream.write(SEVERITY);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(CODE_FLAG);
    this.outputStream.write(SQL_STATE);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(MESSAGE_FLAG);
    this.outputStream.write(this.errorMessage);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.writeByte(NULL_TERMINATOR);
    this.outputStream.flush();
  }

  @Override
  public byte getIdentifier() {
    return 'E';
  }

  @Override
  protected String getMessageName() {
    return "Error";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat(
        "Length: {0}, "
            + "Error Message: {1}")
        .format(new Object[]{
            this.length,
            new String(this.errorMessage, UTF8)
        });
  }

  private static byte[] getMessageFromException(Exception e) {
    return (e.getMessage() == null ? e.getClass().getName() : e.getMessage())
        .getBytes(UTF8);
  }
}
