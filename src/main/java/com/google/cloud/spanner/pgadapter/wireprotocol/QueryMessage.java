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

package com.google.cloud.spanner.pgadapter.wireprotocol;

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.statements.PSQLStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.CommandCompleteResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.RowDescriptionResponse;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executes a simple statement.
 */
public class QueryMessage extends ControlMessage {

  private static final Logger logger = Logger.getLogger(QueryMessage.class.getName());

  protected static final char IDENTIFIER = 'Q';

  private boolean skipSet;
  private String body;
  private IntermediateStatement statement;

  public QueryMessage(ConnectionHandler connection) throws Exception {
    super(connection);

    body = this.readAll();
    skipSet = body.startsWith("SET ");

    logger.log(Level.FINE, "query: " + body);

    if (skipSet) {
      statement = null;
    } else {
      if (!connection.getServer().getOptions().isPSQLMode()) {
        this.statement = new IntermediateStatement(
            body,
            this.connection
        );
      } else {
        this.statement = new PSQLStatement(
            body,
            this.connection
        );
      }

      logger.log(Level.FINE, "updated query: " + this.statement.getSql());

      this.connection.addActiveStatement(this.statement);
    }
  }

  @Override
  protected void sendPayload() throws Exception {
    if (skipSet) {
      logger.log(Level.INFO, "skip set: " + body);
      new CommandCompleteResponse(this.outputStream, "SET").send();
      new ReadyResponse(this.outputStream, ReadyResponse.Status.IDLE).send();
    } else {
      this.statement.execute();
      this.handleQuery();
      this.connection.removeActiveStatement(this.statement);
    }
  }

  @Override
  protected String getMessageName() {
    return "Query";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat(
        "Length: {0}, SQL: {1}")
        .format(new Object[]{this.length, body});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public IntermediateStatement getStatement() {
    return this.statement;
  }

  /**
   * Simple Query handler, whcih examined the state of the statement and processes accordingly
   * (if error, handle error, otherwise sends the result and if contains result set,
   * send row description)
   *
   * @throws Exception
   */
  public void handleQuery() throws Exception {
    if (this.statement.hasException()) {
      this.handleError(this.statement.getException());
    } else {
      if (this.statement.containsResultSet()) {
        new RowDescriptionResponse(this.outputStream,
            this.statement,
            this.statement.getStatementResult().getMetaData(),
            this.connection.getServer().getOptions(),
            QueryMode.SIMPLE).send();
      }
      this.sendSpannerResult(this.statement, QueryMode.SIMPLE, 0L);
      new ReadyResponse(this.outputStream, ReadyResponse.Status.IDLE).send();
    }
    this.connection.cleanUp(this.statement);
  }
}
