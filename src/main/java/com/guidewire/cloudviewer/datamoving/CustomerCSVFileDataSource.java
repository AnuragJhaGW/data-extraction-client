package com.guidewire.cloudviewer.datamoving;

import com.guidewire.tools.benchmarking.CSVFileDefinition;
import com.guidewire.tools.benchmarking.ColumnDef;
import com.guidewire.tools.benchmarking.DataExtractor;
import com.guidewire.tools.benchmarking.QueryDefinition;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A data source definition for customer csv data files. It reads a csv data file and constructs data to be sent to the server
 * It also outputs appropriate error messages to allow customers to clean up their data
 */
public class CustomerCSVFileDataSource implements RowDataSource {
  BufferedReader reader;
  BufferedWriter badFileWriter;
  private boolean headersWrittenToBadFile = false;
  CSVParser csvParser;
  boolean wasNull = false;
  CSVFileDefinition csvFileDefinition;
  Iterator<CSVRecord> csvRecordIterator;
  Map<String, String> latestRow;
  DataExtractor extractor = null;
  long currentRow = 0;
  // successfulHeaders will be set to false if we cannot match up the header row with the list of expected columns
  boolean successfulHeaders = true;
  boolean dataSourceClosed = false;
  private CSVRecord currentRecord;

  public CustomerCSVFileDataSource(File customerCsvFile, DataExtractor extractor, CSVFileDefinition csvFileDefinition) throws IOException {
    // for now I'm setting this to always write the badfile. We can reevaluate after customers use it for a while - we could add a parameter to this to turn it off.
    initialize(customerCsvFile, extractor, csvFileDefinition, true);
  }

  public CustomerCSVFileDataSource(File customerCsvFile, DataExtractor extractor, CSVFileDefinition csvFileDefinition, boolean writeBadData) throws IOException {
    initialize(customerCsvFile, extractor, csvFileDefinition, writeBadData);
  }

  // used for testing only
  public CustomerCSVFileDataSource(InputStream inputStream, DataExtractor extractor, CSVFileDefinition csvFileDefinition) throws IOException {
    this.extractor = extractor;
    this.csvFileDefinition = csvFileDefinition;
    initializeCsvParser(inputStream);
    successfulHeaders = readHeader();
    badFileWriter = null;
  }

  public boolean successfullyReadHeaders() {
    return successfulHeaders;
  }

  @Override
  public QueryDefinition getQueryDefinition() {
    return csvFileDefinition.getQueryDefinition();
  }

  @Override
  public String getName() {
    return csvFileDefinition.getName();
  }

  @Override
  public List<ColumnDef> getColumns() {
    return csvFileDefinition.getColumns();
  }

  @Override
  public boolean next() throws SQLException, IOException {
    return readNextRow();
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException, ParseException {
    if (isColumnNotInFile(columnLabel)) return null;
    wasNull = wasNull(columnLabel);
    return csvFileDefinition.getColumnDefForColumnName(columnLabel).getDate(latestRow.get(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException, ParseException {
    if (isColumnNotInFile(columnLabel)) return null;
    wasNull = wasNull(columnLabel);
    return csvFileDefinition.getColumnDefForColumnName(columnLabel).getTimestamp(latestRow.get(columnLabel));
  }

  @Override
  public Double getDouble(String columnLabel) throws SQLException, ParseException {
    if (isColumnNotInFile(columnLabel)) return null;
    wasNull = wasNull(columnLabel);
    return csvFileDefinition.getColumnDefForColumnName(columnLabel).getDouble(latestRow.get(columnLabel));
  }

  @Override
  public Integer getInt(String columnLabel) throws SQLException, ParseException {
    if (isColumnNotInFile(columnLabel)) return null;
    wasNull = wasNull(columnLabel);
    return csvFileDefinition.getColumnDefForColumnName(columnLabel).getInt(latestRow.get(columnLabel));
  }

  @Override
  public String getString(String columnLabel) throws SQLException, ParseException {
    if (isColumnNotInFile(columnLabel)) return "";
    wasNull = wasNull(columnLabel);
    return csvFileDefinition.getColumnDefForColumnName(columnLabel).getString(latestRow.get(columnLabel));
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

  public boolean columnNotIncludedInFiile(String columnLabel) {
    return latestRow.get(columnLabel) == null;
  }

  public boolean wasNull(String columnLabel) throws SQLException {
    return csvFileDefinition.getColumnDefForColumnName(columnLabel).wasNull(latestRow.get(columnLabel));
  }

  // returns true iff the implementation knows how many rows should be returned.
  public boolean expectedRowsKnown() {
    return false;
  }

  private void initialize(File customerCsvFile, DataExtractor extractor, CSVFileDefinition csvFileDefinition, boolean writeBadData) throws IOException {
    this.extractor = extractor;
    this.csvFileDefinition = csvFileDefinition;
    initializeCsvParserFromFile(customerCsvFile, extractor);
    successfulHeaders = readHeader();
    if (writeBadData) {
      badFileWriter = openBadFileForWrite(createBadDataFile(customerCsvFile));
    }  else {
      badFileWriter = null;
    }
  }

  private void initializeCsvParserFromFile(File customerCsvFile, DataExtractor extractor) throws IOException {
    InputStream inputStream = openCustomerCsvFileForRead(customerCsvFile, extractor);
    initializeCsvParser(inputStream);
  }

  private void initializeCsvParser(InputStream inputStream) throws IOException {
    reader = new BufferedReader(new InputStreamReader(new BOMInputStream(inputStream), "UTF-8"));
    csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader().withIgnoreSurroundingSpaces());
  }

  // Returns the number of rows we expect to find in this RowDataSet.  For implementations that don't have
  // access to this information, return -1.
  public Long getExpectedRows() {
    return -1l;
  }

  @Override
  public long getCurrentRowNumber() {
    return currentRow;
  }

  @Override
  public void writeToBadFile(DataExtractor extractor) {
    String rawRow = constructRawRow();
    writeRawRowToBadFile(extractor, rawRow);
  }

  @Override
  public boolean rowSuccessfullyRead() {
    return latestRow != null;
  }

  private void writeRawRowToBadFile(DataExtractor extractor, String rawRow) {
    if (badFileWriter == null) {
      return;
    }
   writeHeadersToBadFile();
    try {
      badFileWriter.write(rawRow);
      badFileWriter.newLine();
    } catch (IOException e) {
      extractor.getDataExtractorLog().info("Failed to write badfile row for : " + rawRow);
    }
  }

  @Override
  public void close() throws SQLException, IOException {
    if (badFileWriter != null) {
      badFileWriter.close();
    }
    if (reader == null) return;
    reader.close();
    dataSourceClosed = true;
  }

    // Used in testing
  public DataExtractor getExtractor() {
    return extractor;
  }

  /**
   * Read the header line and create the set of columns that will define the output.
   *
   * We want to be permissive, but not too permissive.  For example, the first column is supposed to be
   * an id column.  It could be named "Id", or "id" or even "PolicyId", and we should accept any of those.
   * But it could even be absent, and in any case, we cannot rely on it actually being a primary key - the
   * DES will need to treat the policy number as the true primary key.
   *
   * Returns true iff we were able to successfully read the headers
   *
   * @throws java.io.IOException
   */
  private boolean readHeader() throws IOException {
    Map<String, Integer> headerMap = csvParser.getHeaderMap();
    // increment the current row for the header row as well as data rows because we want to display the actual row
    // number in the file for any data rows that contain errors.
    currentRow++;
    csvRecordIterator = csvParser.iterator();

    return csvFileDefinition.validateHeaderRow(headerMap, extractor);
  }

  private boolean readNextRow() throws IOException {
    currentRecord = null;
    List<ColumnDef> columns = getColumns();
    if (dataSourceClosed) return false;
    try {
      if (!csvRecordIterator.hasNext()) {
        return false;
      }
      currentRecord = csvRecordIterator.next();
      while (!currentRecord.isConsistent()) {
        int expected = csvFileDefinition.getNumberOfColumnsInHeaderRow();
        int actual = currentRecord.size();
        StringBuilder sb = new StringBuilder("Row [")
                .append(currentRow)
                .append("]  contains the wrong number of fields; expected: [")
                .append(expected)
                .append("] got [")
                .append(actual)
                .append("]; ");
        if (actual > expected) {
            sb.append("it may have an unquoted field containing a comma or have mismatched quotes, ");
        } else {
          sb.append("at least one column is missing from this record ");
        }
        if (badFileWriter != null) {
          sb.append("skipping and writing to bad file ");
        }
        sb.append(constructErrorMessageForRow(currentRecord));
        error(sb.toString());
        writeRawRowToBadFile(extractor, constructRawRow());
        if (!csvRecordIterator.hasNext()) {
          return false;
        }
        currentRecord = csvRecordIterator.next();
        currentRow++;
      }
    } catch (RuntimeException re) {
      // if we get this runtime exception something is badly wrong and we should stop processing. Usually happens with an unclosed quote that runs to the end of the file
      if (re.toString().contains("EOF reached before encapsulated token finished")) {
        error("current row is [" + currentRow + "], this row has an open quote without a corresponding close quote:  " + re.toString());
        return false;
      }  else {
        error("current row is [" + currentRow + "], this row has error:  " + re.toString());
      }
    }
    try {
      if (currentRecord == null) {
        // the parser didn't parse the row, but it will try to parse the next row so we want to return true
        latestRow = null;
        return true;
      }

      latestRow = new HashMap<>(columns.size());
      csvFileDefinition.parseRow(latestRow, currentRecord);
      currentRow++;
      return true;
    }  catch (Exception e) {
      return writeExceptionMessage(currentRecord, columns, e);
    }
  }

  private boolean writeExceptionMessage(CSVRecord next, List<ColumnDef> columns, Exception e) {
    info(e.toString());
    if (next != null && columns != null) {
      StringBuilder sb = constructErrorMessageForRow(next);
      info("current row is [" + currentRow + "]");
      info("row.length [" + next.size() + "] columns size [" + columns.size() + "] row before error " + sb.toString());
    }
    // We're returning true because we have some data in latestRow, and the code that gets the
    // columns from latestRow will be able to give a better error message than we can here.
    // Also, we want to continue reading the remaining rows in the file.
    return true;
  }

  private void error (String message) {
    if (extractor != null) {
      extractor.getDataExtractorLog().error(message);
    } else {
      System.out.println(message);
    }
  }

  private void info(String message) {
    if (extractor != null) {
      extractor.getDataExtractorLog().info(message);
    } else {
      System.out.println(message);
    }
  }


  private StringBuilder constructErrorMessageForRow(CSVRecord row) {
    int maxMismatchedQuoteStringLength = 100;
    int maxFieldLength = 20;
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i=0; i<row.size(); i++) {
      if (i>0) {
        sb.append(", ");
      }
      String[] carriageReturns = row.get(i).split("\n");
      if (carriageReturns.length > 1) {
        String trimmedField = row.get(i);
        if (row.get(i).length() > maxMismatchedQuoteStringLength) {
          trimmedField = row.get(i).substring(0, maxMismatchedQuoteStringLength) + "...";
        }
        extractor.getDataExtractorLog().error("Row may contain mismatched quotes - field contains carriage returns [" + trimmedField + "]");
      }
      if (row.get(i).length() < maxFieldLength) {
        sb.append(row.get(i));
      } else {
        sb.append(row.get(i).substring(0, maxFieldLength))
                .append("...");
      }
    }
    sb.append("]");
    return sb;
  }

  private void writeHeadersToBadFile() {
    if (headersWrittenToBadFile) {
      return;
    }
    StringBuilder stringBuilder = new StringBuilder();
    try {
      CSVPrinter csvPrinter = new CSVPrinter(stringBuilder, CSVFormat.DEFAULT);
      csvPrinter.printRecord(csvParser.getHeaderMap().keySet());
      badFileWriter.write(stringBuilder.toString());
      badFileWriter.newLine();
      headersWrittenToBadFile = true;
    } catch (IOException e) {
      extractor.getDataExtractorLog().info("Failed to write badfile column headers " );
    }
  }

  private String constructRawRow() {
    StringBuilder sb = new StringBuilder();
    try {
    CSVPrinter csvPrinter = new CSVPrinter(sb, CSVFormat.DEFAULT);
      csvPrinter.printRecord(currentRecord);
    } catch (IOException e) {

    }
    return sb.toString();
  }

  private boolean isColumnNotInFile(String columnLabel) {
    if (columnNotIncludedInFiile(columnLabel) && !csvFileDefinition.getColumnDefForColumnName(columnLabel).isRequired()) {
      wasNull = true;
      return true;
    }
    return false;
  }

  private BufferedWriter openBadFileForWrite(File badFile) {
    BufferedWriter bufferedWriter = null;
    try {
      bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(badFile)));
    } catch (IOException e) {
      extractor.getDataExtractorLog().info("Policy bad file [" + badFile.getName() + "] could not be created, or could not be written.");
    }
    return bufferedWriter;
  }

  private static InputStream openCustomerCsvFileForRead(File customerCsvFile, DataExtractor extractor) throws FileNotFoundException {
    extractor.getDataExtractorLog().info("Verifying policy csv file [" + customerCsvFile.getName() + "] exists and is readable");
    if (!customerCsvFile.exists() || !customerCsvFile.canRead() || !customerCsvFile.isFile()) {
      extractor.getDataExtractorLog().info("Customer csv file [" + customerCsvFile.getName() + "] does not exist, or cannot be read.");
      return null;
    }
    return new BOMInputStream(new FileInputStream(customerCsvFile));
  }

  private static File createBadDataFile(File policyFile) {
    return new File(policyFile.getPath().concat(".bad"));
  }

}
