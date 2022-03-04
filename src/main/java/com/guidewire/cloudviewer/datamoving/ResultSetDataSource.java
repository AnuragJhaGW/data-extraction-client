package com.guidewire.cloudviewer.datamoving;

import com.guidewire.tools.benchmarking.ColumnDef;
import com.guidewire.tools.benchmarking.DataExtractor;
import com.guidewire.tools.benchmarking.QueryDefinition;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

/**
 * Class description...
 */
public class ResultSetDataSource implements RowDataSource {

  private ResultSet resultSet;
  private QueryDefinition queryDefinition;
  private long timeToRun = -1;

  public ResultSetDataSource(ResultSet rs, QueryDefinition qd) {
    resultSet = rs;
    queryDefinition = qd;
  }

  @Override
  public QueryDefinition getQueryDefinition() {
    return queryDefinition;
  }

  @Override
  public String getName() {
    return queryDefinition.getName();
  }

  @Override
  public List<ColumnDef> getColumns() {
    return queryDefinition.getColumns();
  }

  @Override
  public boolean next() throws SQLException {
    return resultSet.next();
  }

  @Override
  public void close() throws SQLException {
    resultSet.close();
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return resultSet.getDate(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return resultSet.getTimestamp(columnLabel);
  }

  @Override
  public Double getDouble(String columnLabel) throws SQLException {
    return resultSet.getDouble(columnLabel);
  }

  @Override
  public Integer getInt(String columnLabel) throws SQLException {
    return resultSet.getInt(columnLabel);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return resultSet.getString(columnLabel);
  }

  @Override
  public boolean wasNull() throws SQLException {
    return resultSet.wasNull();
  }

  @Override
  public void setTimeToRun(long time) {
    timeToRun = time;
  }

  @Override
  public long getTimeToRun() {
    return timeToRun;
  }

  // returns true iff the implementation knows how many rows should be returning.
  public boolean expectedRowsKnown() {
    return false;
  }

  // Returns the number of rows we expect to find in this RowDataSet.  For implementations that don't have
  // access to this information, return -1.
  public Long getExpectedRows() {
    return -1L;
  }

  // Always return zero for this type of data source. The current row doesn't really make sense for a query upload.
  @Override
  public long getCurrentRowNumber() {
      return 0;
  }

  @Override
  public void writeToBadFile(DataExtractor extractor) {
    throw new UnsupportedOperationException("Write to bad file is not supported for this type of upload");
  }

  @Override
  public boolean rowSuccessfullyRead() {
    return true;
  }
}
