package com.guidewire.cloudviewer.datamoving;

import com.guidewire.tools.benchmarking.ColumnDef;
import com.guidewire.tools.benchmarking.DataExtractor;
import com.guidewire.tools.benchmarking.QueryDefinition;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * Interface description...
 *
 * @author rvernick
 */
public interface RowDataSource {
  QueryDefinition getQueryDefinition();

  String getName();

  List<ColumnDef> getColumns();

  boolean next() throws SQLException, IOException;

  void close() throws SQLException, IOException;

  Date getDate(String date) throws SQLException, ParseException;
  Timestamp getTimestamp(String timestamp) throws SQLException, ParseException;
  Double getDouble(String value) throws SQLException, ParseException;
  Integer getInt(String integer) throws SQLException, ParseException;
  String getString(String s) throws SQLException, ParseException;

  boolean wasNull() throws SQLException;

  // timeToRun indicates how long the query that created this data source took to run.  For any data source
  // that did not involve a query, it should be -1.
  public void setTimeToRun(long timeToRun);
  public long getTimeToRun();

  // returns true iff the implementation knows how many rows should be returning.
  public boolean expectedRowsKnown();

  // Returns the number of rows we expect to find in this RowDataSet.  For implementations that don't have
  // access to this information, return -1.
  public Long getExpectedRows();

  // For csv data types this returns the row number of the current line in the file. Not implemented for query
  // data because it doesn't make sense for that type of data source.
  public long getCurrentRowNumber();

  // Write any row that contains errors to a "bad file" that can be fixed and re-uploaded. Currently only implemented
  // for CustomerCSVFileDataSource because that is the only data source that does validation of the row contents.
  public void writeToBadFile(DataExtractor extractor);

  boolean rowSuccessfullyRead();
}
