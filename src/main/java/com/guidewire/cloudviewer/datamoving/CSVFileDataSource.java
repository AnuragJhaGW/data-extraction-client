package com.guidewire.cloudviewer.datamoving;

import com.guidewire.tools.benchmarking.ColumnDef;
import com.guidewire.tools.benchmarking.DataExtractor;
import com.guidewire.tools.benchmarking.DataExtractorLog;
import com.guidewire.tools.benchmarking.QueryDefinition;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Class description...
 */
public class CSVFileDataSource implements RowDataSource {
  QueryDefinition queryDefinition = new QueryDefinition();
  BufferedReader reader;
  CSVParser parser;
  boolean wasNull = false;
  Map<String, String> latestRow;
  Map<String, ColumnDef> columnMap = new HashMap<>();
  DataExtractor extractor = null;
  long currentRow = 0;
  long expectedRows = -1;
  private Iterator<CSVRecord> csvRecordIterator;

  public CSVFileDataSource(String name, InputStream stream) throws IOException {
    queryDefinition.setName(name);
    reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
    parser = new CSVParser(reader, CSVFormat.DEFAULT);
    csvRecordIterator = parser.iterator();
    readHeader();
  }

  public CSVFileDataSource(String name, InputStream stream, DataExtractor extractor) throws IOException {
    this(name, stream);
    this.extractor = extractor;
  }

  private void readHeader() throws IOException {
    String expectedRowsString = csvRecordIterator.next().get(0);
    // Trim out the "Expected Rows:" part.  Note that if the format of the row changes, this line must change.
    int colonIndex = expectedRowsString.indexOf(":");
    String expectedRowsNumber;
    if (colonIndex == -1) {
      throw new IllegalArgumentException("Did not find expected rows header - got [" + expectedRowsString + "]");
    } else {
      expectedRowsNumber = expectedRowsString.substring(colonIndex + 1).trim();
    }
    expectedRows = Long.parseLong(expectedRowsNumber.trim());
    csvRecordIterator.next().get(0);
    CSVRecord names = csvRecordIterator.next();
    CSVRecord types = csvRecordIterator.next();
    for (int i = 0; i < names.size(); i++) {
      if (names.get(i) != null && names.get(i).length() > 0) {
        ColumnDef column = ColumnDef.createDefinition(types.get(i), names.get(i));
        queryDefinition.addColumn(column);
        columnMap.put(column.getName(), column);
      }
    }
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
  public boolean next() throws SQLException, IOException {
    return readNextRow();
  }

  private boolean readNextRow() throws IOException {
    CSVRecord row = null;
    List<ColumnDef> columns = null;
    try {
      row = csvRecordIterator.next();
      if (row.get(0).contains("<data end>")) return false;
      columns = getColumns();
      latestRow = new HashMap<>(columns.size());
      for (int i = 0; i < columns.size(); i++) {
        latestRow.put(columns.get(i).getName(), row.get(i));
      }
      currentRow++;
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      if (extractor != null) {
        DataExtractorLog log = extractor.getDataExtractorLog();
        log.error(e.toString());
        log.info("current row is [" + currentRow + "]");
        log.info("row.length [" + row.size() + "] columns size [" + columns.size() + "]");
        for (int i=0; i<row.size(); i++) {
          log.info("row[" + i + "] is [" + row.get(i) + "]");
        }
        log.info("data file has been truncated");
      } else {
        System.out.println("current row is [" + currentRow + "]");
        System.out.println("row.length [" + row.size() + "] columns size [" + columns.size() + "]");
        for (int i=0; i<row.size(); i++) {
          System.out.println("row[" + i + "] is [" + row.get(i) + "]");
        }
        System.out.println("data file has been truncated");
      }
      return false;
    }
  }

  @Override
  public void close() throws SQLException, IOException {
    if (reader == null) return;
    reader.close();
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException, ParseException {
    wasNull = wasNull(columnLabel);
    return columnMap.get(columnLabel).getDate(latestRow.get(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException, ParseException {
    wasNull = wasNull(columnLabel);
    return columnMap.get(columnLabel).getTimestamp(latestRow.get(columnLabel));
  }

  @Override
  public Double getDouble(String columnLabel) throws SQLException, ParseException {
    wasNull = wasNull(columnLabel);
    return columnMap.get(columnLabel).getDouble(latestRow.get(columnLabel));
  }

  @Override
  public Integer getInt(String columnLabel) throws SQLException, ParseException {
    wasNull = wasNull(columnLabel);
    return columnMap.get(columnLabel).getInt(latestRow.get(columnLabel));
  }

  @Override
  public String getString(String columnLabel) throws SQLException, ParseException {
    wasNull = wasNull(columnLabel);
    return columnMap.get(columnLabel).getString(latestRow.get(columnLabel));
  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  @Override
  public void setTimeToRun(long timeToRun) {
    // For CVS files, timeToRun is always -1
  }

  @Override
  public long getTimeToRun() {
    return -1;
  }

  public boolean wasNull(String columnLabel) throws SQLException {
    return columnMap.get(columnLabel).wasNull(latestRow.get(columnLabel));
  }

  // returns true iff the implementation knows how many rows should be returning.
  public boolean expectedRowsKnown() {
    return true;
  }

  // Returns the number of rows we expect to find in this RowDataSet.  For implementations that don't have
  // access to this information, return -1.
  public Long getExpectedRows() {
    return expectedRows;
  }

  @Override
  public long getCurrentRowNumber() {
    return currentRow;
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
