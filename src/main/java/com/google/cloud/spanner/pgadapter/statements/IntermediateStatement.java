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

package com.google.cloud.spanner.pgadapter.statements;

import com.google.cloud.spanner.jdbc.JdbcConstants;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.metadata.DescribeMetadata;
import com.google.cloud.spanner.pgadapter.metadata.QueryRewritesMetadata;
import com.google.common.base.Preconditions;
import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * Data type to store simple SQL statement with designated metadata. Allows manipulation of
 * statement, such as execution, termination, etc. Represented as an intermediate representation for
 * statements which does not belong directly to Postgres, Spanner, etc.
 */
public class IntermediateStatement {

  private final Statement statement;
  private final String sql;
  private final Connection connection;
  private final String command;

  private ResultType resultType;
  private ResultSet statementResult;
  private boolean hasMoreData;
  private Exception exception;
  private Integer updateCount;
  
  protected boolean executed;

  public IntermediateStatement(String sql, ConnectionHandler connectionHandler, Function<String,String> translateSQL) throws SQLException {
    this(translateSQL.apply(sql), connectionHandler, connectionHandler.getJdbcConnection().createStatement(), translateSQL);
  }

  public IntermediateStatement(String sql, ConnectionHandler connectionHandler, Statement statement, Function<String,String> translateSQL) {

    List<QueryRewritesMetadata> rewrites = Optional.ofNullable(connectionHandler.getServer()).
            map(x->x.getOptions().getQueryRewritesJSON()).orElse(Collections.emptyList());

    this.sql = rewriteQuery(sql, rewrites);
    this.command = parseCommand(sql);
    this.connection = connectionHandler.getJdbcConnection();
    this.statement = statement;
    

    this.executed = false;
    this.exception = null;
    this.resultType = null;
    this.hasMoreData = false;
    this.statementResult = null;
    this.updateCount = null;
  }

  public IntermediateStatement(String sql, ConnectionHandler connectionHandler, Statement statement) {
    this(sql, connectionHandler, statement, x -> x);
  }

  public IntermediateStatement(String sql, ConnectionHandler connectionHandler) throws SQLException {
    this(sql, connectionHandler, x -> x);
  }

  /**
   * Extracts what type of result exists within the statement. In JDBC a statement update count is
   * positive if it is an update statement, 0 if there is no result, or negative if there are
   * results (i.e.: select statement)
   *
   * @param statement The resulting statement from an execution.
   * @return The statement result type.
   * @throws SQLException If getUpdateCount fails.
   */
  private static ResultType extractResultType(Statement statement) throws SQLException {
    switch (statement.getUpdateCount()) {
      case JdbcConstants.STATEMENT_NO_RESULT:
        return ResultType.NO_RESULT;
      case JdbcConstants.STATEMENT_RESULT_SET:
        return ResultType.RESULT_SET;
      default:
        return ResultType.UPDATE_COUNT;
    }
  }

  /**
   * Determines the (update) command that was received from the sql string.
   */
  protected static String parseCommand(String sql) {
    Preconditions.checkNotNull(sql);
    String[] tokens = sql.split("\\s+", 2);
    if (tokens.length > 0) {
      return tokens[0].toUpperCase();
    }
    return null;
  }

  /**
   * Whether this is a bound statement (i.e.: ready to execute)
   *
   * @return True if bound, false otherwise.
   */
  public boolean isBound() {
    return true;
  }

  /**
   * Cleanly close the statement. Does nothing if the statement has not been executed or has no
   * result.
   *
   * @throws Exception if closing fails server-side.
   */
  public void close() throws Exception {
    if (this.getStatementResult() != null) {
      this.getStatementResult().close();
    }
  }

  /**
   * @return True if this is a select statement, false otherwise.
   */
  public boolean containsResultSet() {
    return this.resultType == ResultType.RESULT_SET;
  }

  /**
   * @return True if this statement was executed, False otherwise.
   */
  public boolean isExecuted() {
    return executed;
  }

  /**
   * @return The number of items that were modified by this execution.
   */
  public Integer getUpdateCount() {
    return this.updateCount;
  }

  /**
   * @return True if at some point in execution, and exception was thrown.
   */
  public boolean hasException() {
    return this.exception != null;
  }

  /**
   * @return True if only a subset of the available data has been returned.
   */
  public boolean isHasMoreData() {
    return this.hasMoreData;
  }

  public void setHasMoreData(boolean hasMoreData) {
    this.hasMoreData = hasMoreData;
  }

  public Statement getStatement() {
    return this.statement;
  }

  protected Connection getConnection() {
    return this.connection;
  }
  
  public ResultSet getStatementResult() {
    return this.statementResult;
  }

  public ResultType getResultType() {
    return this.resultType;
  }

  public String getSql() {
    return this.sql;
  }

  public Exception getException() {
    Exception e = this.exception;
    this.exception = null;
    return e;
  }

  /**
   * Runs a specific execution, extracting metadata from that execution (including results and
   * update counts)
   *
   * @throws SQLException If an issue occurred in extracting result metadata.
   */
  protected void executeHelper() throws SQLException {
    this.resultType = IntermediateStatement.extractResultType(this.statement);
    if (this.containsResultSet()) {
      this.statementResult = this.statement.getResultSet();
      this.hasMoreData = this.statementResult.next();
    } else {
      this.updateCount = this.statement.getUpdateCount();
      this.hasMoreData = false;
      this.statementResult = null;
    }
  }

  /**
   * Clean up and save metadata when an exception occurs.
   *
   * @param e The exception to store.
   */
  protected void handleExecutionException(SQLException e) {
    this.exception = e;
    this.hasMoreData = false;
    this.statementResult = null;
    this.resultType = ResultType.NO_RESULT;
  }

  /**
   * Execute the SQL statement, storing metadata.
   */
  public void execute() {
    this.executed = true;
    try {
      this.statement.execute(this.sql);
      this.executeHelper();
    } catch (SQLException e) {
      handleExecutionException(e);
    }
  }

  /**
   * Moreso meant for inherited classes, allows one to call describe on a statement. Since raw
   * statements cannot be described, throw an error.
   */
  public DescribeMetadata describe() throws Exception {
    throw new IllegalStateException("Cannot describe a simple statement "
        + "(only prepared statements and portals)");
  }

  /**
   * Moreso intended for inherited classes (prepared statements et al) which allow the setting of
   * result format codes. Here we dafault to string.
   */
  public short getResultFormatCode(int index) {
    return 0;
  }

  /**
   * @return the extracted command (first word) from the SQL statement.
   */
  public String getCommand() {
    return this.command;
  }

  /**
   * foldLeft application of all rewrites on the sql string
   * @param sql
   * @param rewrites
   * @return the sql with all the rewrites applied in order
   */
  private String rewriteQuery(String sql, List<QueryRewritesMetadata> rewrites) {
    String rewrittenSql = sql;
    for ( QueryRewritesMetadata rewrite : rewrites) {
      rewrittenSql = rewriteQuery(rewrittenSql, rewrite);
    }
    return rewrittenSql;
  }

  private String rewriteQuery(String sql, QueryRewritesMetadata rewrite) {
    return sql.replaceAll(rewrite.getInputPattern(), rewrite.getOutputPattern());
  }
  
  public enum ResultType {UPDATE_COUNT, RESULT_SET, NO_RESULT}
}
