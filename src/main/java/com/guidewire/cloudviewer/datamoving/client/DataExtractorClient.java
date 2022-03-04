package com.guidewire.cloudviewer.datamoving.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.guidewire.cloudviewer.datamoving.*;
import com.guidewire.tools.DataExtractionUtils;
import com.guidewire.tools.benchmarking.*;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.ExceptionUtils;
import org.xml.sax.SAXException;

import javax.net.ssl.SSLHandshakeException;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.ConnectException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DataExtractorClient runs and coordinates interactions between the DataExtractor on the client side and the
 * Guidewire dataExtractionServer.  It will create get/put requests to the server for sending data, logs and exceptions.
 * It will also coordinate the setting of parameters useful to the server as well as getting retrieving queries from
 * the server appropriate to the customer's environment
 */
public class DataExtractorClient {
  private static final int MAX_ITERATIONS_FOR_RUNNING = 1000;
  private static final int DEFAULT_MAX_ATTEMPTS_TO_SEND_RESULTS = 1000;
  public static final String DBMS_PARAMETER = "dbms";
  public static final String IS_WINDOWS_PARAMETER = "isWindows";
  public static final String CLIENT_VERSION = "clientVersion";
  public static final String DB_CONNECTION_URL = "dbConnectionUrl";
  public static final String DB_CONNECTION_USER = "dbConnectionUser";
  public static final String DB_CONNECTION_DB = "dbConnectionDB";
  public static final String DB_CONNECTION_SCHEMA = "dbConnectionSchema";
  public static final String CUSTOMER_CSV_FILETYPE = "csvFileType";
  private static final int MAX_CHARACTERS_TO_SEND = 10000;
  private static final int DEFAULT_MAX_ROWS_TO_SEND_PER_POST = 1000;
  private static final int DEFAULT_MAX_ROWS_TO_SEND_PER_FILE_UPLOAD = 1000;
  private static final int DEFAULT_MAX_ERROR_ROWS_BEFORE_FAILURE = 10000;
  private static final int NUMBER_OF_LOG_FILES_TO_UPLOAD = 3;
  protected static final String AUTHENTICATION_FAILED = "authentication failed";
  public static final String UPLOAD_DISABLED = "The Guidewire server is temporarily not accepting data.  Please try again later.";


  /**
   * The next set of integers defines the commands that are sent to the various controllers on the server.
   * we started out not making these integers unique across the provider controller and upload controller, so there are several ones
   * among them. We are now keeping the integers unique over all of the entries for clarity
   */
  public static final Integer ERROR_UPLOAD = 1;
  public static final Integer LOG_UPLOAD = 1;

  // Commands for the QueryProviderController
  public static final Integer QUERY_REQUEST = 1;
  public static final Integer QUERY_DERIVED_PARAMETERS_REQUEST = 4;
  public static final Integer QUERY_TEST_CONNECTION_REQUEST = 5;
  public static final Integer TEST_TOOL_CMD_REQUEST = 7;
  public static final Integer CSV_CLEAN_DATA_CHECK = 9;
  public static final Integer CUSTOMER_CSV_DEFINITION_REQUEST = 10;
  public static final Integer CUSTOMER_CSV_TEST_DEFINITION_REQUEST = 11;

  // Commands for the QueryUploadController
  public static final Integer QUERY_RESULT_UPLOAD = 1;
  public static final Integer QUERY_SUMMARY = 2;
  public static final Integer QUERY_CSV_UPLOAD = 3;
  public static final Integer QUERY_SUMMARY_CSV = 6;
  public static final Integer TEST_TOOL_CMD_RESULTS = 8;
  public static final Integer CUSTOMER_CSV_UPLOAD = 12;
  public static final Integer CUSTOMER_SUMMARY_CSV = 13;
  public static final Integer CUSTOMER_FILEDEF_UPLOAD = 14;

  private final DataExtractor _dataExtractor;

  private int maxRowsToSend = DEFAULT_MAX_ROWS_TO_SEND_PER_POST;
  private int totalRowsSent = 0;
  private int rowsForCurrentTable = 0;

  // We may enforce a maximum time restriction.  If the current time is ever larger than maxCurrentTime, we should halt
  // whatever we are doing and return.
  private final boolean enforceMaxTime;
  private final long maxCurrentTime;
  private final long maxRunTime;

  private QueryConnectionSummary querySummary;

  // This boolean will be set to true if we have made an attempt to connect to the Guidewire server and have
  // failed with an exception, or with a server error.  Essentially, this is an indicator that we are unlikely
  // to be able to correctly connect and may want to stop attempting to.
  private boolean unsuccessfulConnectAttempt = false;

  // We may get a response from which we can know that no further requests to the server will succeed.  In these cases,
  // we need to know that there's no point in continuing with other queries.
  private boolean serverAcceptingData =  true;
  private Map<String, String> queryDerivedParameters = new HashMap<>();

  // Instructs the DataExtractorClient about whether to print full queries or just the names of queries when
  // a test connection pulls queries from the server.
  protected boolean printFullQueries = true;

  protected boolean sendDatabaseConnectionInfo = true;

  // Used during CSV uploads - the first time we upload a file, we need to check with the server to make sure
  // the tables are clean (that is, that the dates in the process tables are null, or there simply aren't any
  // entries in the process tables).  If there are dates, that indicates that there's already data that has
  // been uploaded, which should never be the case for a CSV upload.  So if we see that, we fail the upload.
  protected boolean csvCheckComplete = false;

  private String gwAuth = null;

  // When we first connect, we set up a client with the auth token in it and then use the same client for
  // all subsequent connections.
  private HttpClient desClient = null;


  public enum UploadType {
    CUSTOMER_CSV(CUSTOMER_CSV_UPLOAD),
    INITIAL_CSV(QUERY_CSV_UPLOAD),
    INCREMENTAL_QUERY_LOAD(QUERY_RESULT_UPLOAD),
    CUSTOMER_CSV_FILEDEF(CUSTOMER_FILEDEF_UPLOAD);

    private final int _command;

    private UploadType(int command) {
      _command = command;
    }

    public int getCommand() {
      return _command;
    }
  }


  public DataExtractorClient(DataExtractor dataExtractor, long start, long maxRunTime) {
    this._dataExtractor = dataExtractor;

    if (maxRunTime > 0) {
      enforceMaxTime = true;
      maxCurrentTime = start + maxRunTime;
      this.maxRunTime = maxRunTime;
    }
    else {
      enforceMaxTime = false;
      maxCurrentTime = -1;
      this.maxRunTime = -1;
    }

    totalRowsSent = 0;
    rowsForCurrentTable = 0;
    querySummary = new QueryConnectionSummary(getUsername());
  }



  public CSVFileDefinition getCSVFileDefinitionFromLocalFile(String fileName, DataExtractor dataExtractor) throws IOException {
    InputStream inputStream = DataExtractionUtils.getInputStreamBasedOnFileName(fileName);
    try {
      return CSVFileDefinition.convertXMLToFileDefinition(inputStream, dataExtractor);
    } catch (SAXException saxe) {
      error("Error parsing file [" + fileName + "] " + saxe.getMessage());
    }  catch (IOException ioe) {
      error("Error reading file [" + fileName + "] " + ioe.getMessage());
    } catch (ParserConfigurationException parsere) {
      error("Error configuring parser for file [" + fileName + "] " + parsere.getMessage());
    }
    return null;
  }


  public void clearConnectionSummary() {
    querySummary = new QueryConnectionSummary(getUsername());
  }

  public DataExtractor getDataExtractor() {
    return _dataExtractor;
  }

  public void setSendDatabaseConnectionInfo(boolean sendConnectionInfo) {
    sendDatabaseConnectionInfo = sendConnectionInfo;
  }


  public boolean unsuccessfulConnectAttempt() {
    return unsuccessfulConnectAttempt;
  }

  public void setPrintFullQueries(boolean setting) {
    printFullQueries = setting;
  }

  /**
   * This method will contact the dataExtractionServer and request a set of queries.  It will then run
   * those queries and post the results back to the dataExtractionServer.  The expected use will be for
   * incremental updates from a customer running the client from a cron job or some similar mechanism.
   */
  public void run() throws IOException, SQLException {
    boolean needToRerun = true;
    int iterations = 0;
    info("Sending logs");
    sendLogs(NUMBER_OF_LOG_FILES_TO_UPLOAD);
    while (needToRerun && iterations++ < MAX_ITERATIONS_FOR_RUNNING && continueSendingData()) {
      needToRerun = false;
      try {
        // should send the previous logs first, then clear and restart them
        info("updating queries");
        prepareToRun();
      } catch (IOException e) {
        error("Error in getting queries", e);
        throw e;  // rethrow the error after logging it
      }
      for (QueryDefinition queryDefinition : _dataExtractor.getQueries()) {
        if (wasNotAbleToSendEntireResultSet(send(queryDefinition))) {
          needToRerun = true;
        }
        if (!continueSendingData())
          break;
      }
    }

    // Complete the summary and send it to the server.  Note that we send a summary even if we are not continuing to
    // send data
    sendSummary(UploadType.INCREMENTAL_QUERY_LOAD);
  }

  public void sendSummary(UploadType uploadType) {
    updateSummary();
    setDatabaseConnectionInfo();
    PostMethod summaryPost = createQuerySummaryPostRequest(uploadType);
    summaryPost.setRequestBody(createQuerySummaryPayload(querySummary));
    sendRequest(summaryPost);
    if (summaryPost.getStatusCode() == HttpStatus.SC_OK) {
      info("Successfully sent query summary");
    } else {
      error("Query summary send failed with status code [" + summaryPost.getStatusCode() + "]");
    }
  }


  /**
   * Post data from the passed in input stream on a CSV file to the dataExtractionServer.  The data will
   * be written to a table based on the name passed in.  In general, we expect this will be used in-house
   * to load data sent by customers.
   *
   * Note that runFromFile() does _not_ call sendSummary to send the final query summary to the server.
   * This must be done from the caller, since we will otherwise send a summary for each file sent, which
   * will in turn result in a separate extract period id for each file, which will be a problem for the
   * staging to ods process (see VEGA-483).
   *
   * @param name   Indicates which table to upload the data to
   * @param stream A stream on a CSV file containing the data to upload.
   */
  public void runFromFile(String name, InputStream stream) {
    // We want to load more lines per http post when we are uploading from a file.
    setMaxRowsToSend(DEFAULT_MAX_ROWS_TO_SEND_PER_FILE_UPLOAD);
    if (!csvCheckComplete && !doCSVCheck()) {
      throw new DataExtractorServerException("CSV check failed, check server tables to make sure they are clean");
    } else {
      csvCheckComplete = true;
    }
    send(name, stream);
  }

  public boolean sendCustomerCsvFileDefinition(CSVFileDefinition fileDefinition) {
    String name = fileDefinition.getName();
    info("Sending data for [" + name + "]");
    boolean needToRerun = true;
    int iterations = 0;
    NameValuePair[] result = getUploadDataFromFileDefinition(fileDefinition);
    while (needToRerun && iterations++ < MAX_ITERATIONS_FOR_RUNNING) {
      needToRerun = false;
      boolean sent = sendData(result, 1, UploadType.CUSTOMER_CSV_FILEDEF);
      if (wasNotAbleToSendEntireResultSet(sent)) {
        needToRerun = true;
        info("Continuing to send data for [" + name + "] - total rows sent: " + totalRowsSent);
      } else {
        info("Completed sending data for [" + name + "] - total rows sent: " + totalRowsSent);
      }
    }
    return true;
  }

  private NameValuePair[] getUploadDataFromFileDefinition(CSVFileDefinition fileDefinition) {
    ArrayList<CSVFileDefinition> csvFileDefinitions = new ArrayList<>(1);
    csvFileDefinitions.add(fileDefinition);
    CSVFileDefinitionJSON csvFileDefinitionJSON = new CSVFileDefinitionJSON();
    csvFileDefinitionJSON.setFileDefinitions(csvFileDefinitions);
    NameValuePair[] result = new NameValuePair[1];
    Gson gson = new Gson();
    String queryResultGson = gson.toJson(csvFileDefinitionJSON);
    result[0] = new NameValuePair("results", queryResultGson);
    return result;
  }

  public boolean sendCustomerCsvFile(CustomerCSVFileDataSource policyDataSource) {
    String name = policyDataSource.getName();
    info("Sending data for [" + name + "]");
    boolean needToRerun = true;
    int iterations = 0;
    while (needToRerun && iterations++ < MAX_ITERATIONS_FOR_RUNNING && continueSendingData()) {
      needToRerun = false;
      if (wasNotAbleToSendEntireResultSet(sendData(policyDataSource, UploadType.CUSTOMER_CSV))) {
        needToRerun = true;
        info("Continuing to send data for [" + name + "] - total rows sent: " + totalRowsSent);
      } else {
        info("Completed sending data for [" + name + "] - total rows sent: " + totalRowsSent);
      }
    }
    return true;
  }

  public boolean validateCustomerCsvFile(CustomerCSVFileDataSource csvFileDataSource) {
    String name = csvFileDataSource.getName();
    info("Validating data for [" + name + "]");
    int iterations = 0;
    while (iterations++ < MAX_ITERATIONS_FOR_RUNNING && continueSendingData()) {
      try {
        QueryResult data = buildResultPackage(csvFileDataSource);
        if (data.getRowCount() == 0) return true;
      } catch (SQLException e) {
        // this will never happen for this data source because the source reads a file, but the method throws it
        error("Error reading from database", e);
        return false;
      } catch (ParseException e) {
        error("Error reading parsing data", e);
        return false;
      } catch (IOException e) {
        error("Error reading data source", e);
        return false;
      }
    }
    return true;
  }



  public void prepareToRun() throws IOException {
    updateDBMSParameter();
    updateQueryDerivedParameters();
    updateQueries(false);
  }

  public List<CSVFileDefinition> prepareToRunCustomerCSVFile(boolean isTestConnection) throws IOException {
    info("Retrieving csv file definition from [" + getDataExtractionServerURL() + "]");
    HttpMethod httpMethod;
    if (isTestConnection) {
      httpMethod = createFileDefinitionGetRequest(CUSTOMER_CSV_TEST_DEFINITION_REQUEST);
    } else {
      httpMethod = createFileDefinitionGetRequest(CUSTOMER_CSV_DEFINITION_REQUEST);
    }
    sendRequest(httpMethod);
    List<CSVFileDefinition> fileDefs = CSVFileDefinition.convertResponseBodyToCSVFileDefinitions(httpMethod.getResponseBodyAsStream());
    logReceiptOfFileDefs(fileDefs);
    return fileDefs;
  }

  public void updateQueries(boolean isTestConnection) throws IOException {
    setQueriesFor(isTestConnection);
  }


  public TestToolCommands requestTestToolCommands(boolean isWindows) throws IOException {
    HttpMethod httpMethod = createTestToolGetRequest(TEST_TOOL_CMD_REQUEST, isWindows);
    sendRequest(httpMethod);
    TestToolCommands commands = unpackTestToolCommands(httpMethod.getResponseBodyAsStream());
    return commands;
  }

  public void sendTestToolResults(TestToolResults results) {
    try {
      PostMethod upload = createDataPostRequest(TEST_TOOL_CMD_RESULTS);
      upload.setRequestBody(createTestToolResultsPayload(results));
      sendRequest(upload);
    } catch (Exception e) {
      error("Error uploading test tool results", e);
    }
  }



  public void updateDBMSParameter() {
    setQueryDerivedParameter(DBMS_PARAMETER, getDataExtractor().getDBType());
  }


  public void logClientInfo() throws IOException {
    HttpMethod httpMethod = createClientInfoRequest();
    sendRequest(httpMethod);
    InputStream inputStream = httpMethod.getResponseBodyAsStream();
    String clientInfoJson = restoreDoubleQuotes(IOUtils.toString(inputStream));
    ClientInfo clientInfo = new Gson().fromJson(clientInfoJson, ClientInfo.class);
    String message = "Running for client: " + clientInfo.displayName + "(" + clientInfo.cbsclientid + ")";
    _dataExtractor.getDataExtractorLog().info(message);
  }

  // todo: share with DES
  private static class ClientInfo {
    private long cbsclientid = -1;
    private String displayName = null;
  }


  public void updateQueryDerivedParameters() throws IOException {
    info("Retrieving version information query from [" + getDataExtractionServerURL() + "]");
    HttpMethod httpMethod = createQueryDefinitionGetRequest(QUERY_DERIVED_PARAMETERS_REQUEST);
    sendRequest(httpMethod);
    List<QueryDefinition> queryDefinitions = convertResponseBodyToQueryDefinitions(httpMethod.getResponseBodyAsStream());
    logReceipt(queryDefinitions);
    for (QueryDefinition queryDefinition : queryDefinitions) {
      try {
        PreparedStatement query = _dataExtractor.createPreparedStatement(queryDefinition);
        if (!query.execute()) {
          error("Failed execution of " + _dataExtractor.getPreparedStatementQuery(queryDefinition));
        }
        ResultSet resultSet = query.getResultSet();
        if (resultSet.next()) {
          setQueryDerivedParameter(queryDefinition.getName(), resultSet.getString(1));
        } else {
          error("No rows returned for " + _dataExtractor.getPreparedStatementQuery(queryDefinition));
        }
      } catch (SQLException e) {
        error("Could not get query request arguments", e);
      }
    }

    // Set the version property
    setQueryDerivedParameter(CLIENT_VERSION, _dataExtractor.getGuidewireClientVersion());

    // Set the database connection properties
    setDatabaseConnectionInfo();
  }

  public void setCustomerCSVFileTypeParameter(String optionValue) {
    setCustomerCSVFileParameter(optionValue);
  }


  public QueryResult buildResultPackage(RowDataSource dataSource) throws SQLException, ParseException, IOException {
    QueryResult result = new QueryResult();
    result.setName(dataSource.getName());
    result.setLakeOnly(dataSource.getQueryDefinition().isLakeOnly());
    List<ColumnDef> columns = dataSource.getColumns();
    result.setColumns(columns);
    int columnsPerResult = columns.size();
    // This keeps track of rows that we did not get an error for.  This is used primarily for .csv files that
    // we are reading, where we return results for rows that succeed and omit rows that fail.
    int successfulRows = 0;
    while (dataSource.next()) {
      if (dataSource.rowSuccessfullyRead()) {
        StringBuilder rowErrorMessage = null;
        // We want to limit the number of rows to ensure the data payload is not too big
        // At the same time, we want to get to a new update time so we don't send the same data
        // repeatedly
        QueryResult.ResultRow resultRow = QueryResult.newResultRow(columnsPerResult);
        ColumnDef currentColumn;
        for (ColumnDef column : columns) {
          currentColumn = column;
          try {
            resultRow.add(column.getOutputResults(dataSource));
          } catch (Exception e) {
            // This is the heart of how we manage handling bad data, particularly for .csv files.  When parsing
            // data that we expect to be of a certain type (usually date or numeric fields), we may get a parse
            // error.  This is often because of some problem in the data that we're reading.  If this happens,
            // we want to give the customer the most information possible about the problem info so they can
            // find the problem and fix it. We can also get errors if required values are missing. We do minimal checks for these;
            // mostly insure that string lengths are greater than zero. For data types that require parsing an input string there is
            // an additional check that the value must parse correctly, but for STRING values we only check for a non-empty string.

            // Note that we don't stop processing immediately - we want to record all of the errors for every column in the row
            if (rowErrorMessage == null) {
              rowErrorMessage = new StringBuilder("Error handling data source row [")
                .append( dataSource.getCurrentRowNumber()).append( "] with data [");
              StringBuilder sb = constructRawRow(resultRow);
              rowErrorMessage.append(sb.toString())
                .append("]");
            }
            rowErrorMessage.append(", column [")
              .append(currentColumn.getName())
              .append("], error is [")
              .append(e.getLocalizedMessage())
              .append("]");
          }
        }
        if (rowErrorMessage == null) {
          result.addRow(resultRow);
          successfulRows++;
        } else {
          dataSource.writeToBadFile(_dataExtractor);
          _dataExtractor.getDataExtractorLog().error(rowErrorMessage.toString());
          List<String> errors = _dataExtractor.getDataExtractorLog().getErrorMessages();
          if (errors.size() > DEFAULT_MAX_ERROR_ROWS_BEFORE_FAILURE) {
            _dataExtractor.getDataExtractorLog().displayToStdOut("The following errors occurred during execution.  Please check the log file for more details.");
            for (String error : errors) {
              _dataExtractor.getDataExtractorLog().displayToStdOut("ERROR: " + error);
            }
            _dataExtractor.getDataExtractorLog().displayToStdOut("Too many errors - exiting.  Please check the log file for more details.");
            return result;
          }
        }
        if (successfulRows >= getMaxRowsToSend()) {
          result.setWasCutShort(true);
          break;
        }
      }
    }
    result.setRowCount(successfulRows);
    if (!result.wasCutShort()) {
      dataSource.close();
      result.setChecksum(getChecksum(dataSource.getQueryDefinition()));
    }
    result.setQueryTime(dataSource.getTimeToRun());

    List<String> errors = _dataExtractor.getDataExtractorLog().getErrorMessages();
    if (errors.size() > 0) {
      _dataExtractor.getDataExtractorLog().displayToStdOut("The following errors occurred during execution.  Please check the log file for more details.");
      for (String error : errors) {
        _dataExtractor.getDataExtractorLog().displayToStdOut("ERROR: " + error);
      }
    }

    return result;
  }


  ///////////////////////////////////////       Private


  private String getUsername() {
    return _dataExtractor.getUsername();
  }

  private String getCompanyPassword() {
    return _dataExtractor.getGuidewirePassword();
  }


  private boolean sendDatabaseConnectionInfo() {
    return sendDatabaseConnectionInfo;
  }


  private String getDataExtractionServerURL() {
    return _dataExtractor.getUploadURL();
  }



  private int getMaxRowsToSend() {
    return maxRowsToSend;
  }

  private void setMaxRowsToSend(int rows) {
    maxRowsToSend = rows;
  }


  /**
   * Performs checks to decide if we should continue sending data to the server.
   *
   * Checks to see if there is a maximum time restriction in force, and if there is, whether we have passed it.
   * Checks to see if we have set the flag that says to stop sending data.
   *
   * @return true iff we should continue contacting the server and sending data
   */
  private boolean continueSendingData() {
    if (!serverAcceptingData) {
      return false;
    }

    return !maxTimeLimitPassed();
  }

  private boolean maxTimeLimitPassed() {
    if (enforceMaxTime) {
      long now = System.currentTimeMillis();
      if (now > maxCurrentTime)
        return true;
      else
        return false;
    } else {
      return false;
    }
  }

  private void updateSummary() {
    querySummary.setEndtime(System.currentTimeMillis());

    boolean continueSendingData = continueSendingData();
    if (continueSendingData) {
      // no update needed
    } else {
      if (!serverAcceptingData) {
        querySummary.addMessage("Upload disabled on server, sending has stopped.");
      }

      if (maxTimeLimitPassed()) {
        querySummary.addMessage("Max time limit has expired.  Max time limit was [" + maxRunTime + "]");
      }
    }
  }

  private boolean wasNotAbleToSendEntireResultSet(boolean send) {
    return !send;
  }


  /**
   * Returns true iff the database is clean and ready to receive an initial load for this customer.
   *
   * @return true iff clean.
   */
  private boolean doCSVCheck(){
    info("Requesting CSV check from [" + getDataExtractionServerURL() + "]");
    HttpMethod httpMethod = createCSVCheckGetRequest(CSV_CLEAN_DATA_CHECK);
    sendRequest(httpMethod);
    try {
      return convertCSVCheckResult(httpMethod.getResponseBodyAsString());
    } catch (IOException e) {
      error("Couldn't get CSV check [" + e.getLocalizedMessage() + "]", e, false);
      return false;
    }
  }

  private boolean convertCSVCheckResult(String json) throws IOException {
    Gson gson = new Gson();
    return gson.fromJson(restoreDoubleQuotes(json), Boolean.class);
  }


  /**
   * Executes the query and sends the resulting data to the server.  Returns true iff all result sets
   * are completely sent.
   *
   * @param queryDefinition the query to execute.
   * @return true if the data is successfully sent, false if not.
   */
  private boolean send(QueryDefinition queryDefinition) throws SQLException {
    ResultSetDataSource resultSetDataSource = null;
    try {
      String query = _dataExtractor.getPreparedStatementQuery(queryDefinition);
      List<String> params = _dataExtractor.getPreparedStatementArguments(queryDefinition);
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < params.size(); i++) {
        if (i > 0)
          sb.append(", ");
        sb.append(params.get(i));
      }
      info("Running query [" + query + "] with params [" + sb.toString() + "]");
      DataExtractor.QueryRun queryRun = runQuery(queryDefinition);
      // If no data comes back from executing the query, we're done.
      if (queryRun == null) {
        info("No data to send from query");
        return true;
      }

      info("Sending data from query [" + query + "]");
      resultSetDataSource = new ResultSetDataSource(queryRun.getResultSet(), queryDefinition);
      resultSetDataSource.setTimeToRun(queryRun.getTimeToRun());
      rowsForCurrentTable = 0;
      int iterations = 0;
      boolean result = false;
      int previousRowsSent = 0;
      while (iterations++ < MAX_ITERATIONS_FOR_RUNNING && continueSendingData() && result == false) {
        result = sendData(resultSetDataSource, UploadType.INCREMENTAL_QUERY_LOAD);
        if (result) {
          info("Completed sending data - total rows sent: " + totalRowsSent);
        } else {
          info("Continuing to send data - total rows sent: " + totalRowsSent);
        }

        if (previousRowsSent == totalRowsSent && result == false) {
          // If we get the same numbers twice in a row, there's something going on and it's time to kill
          // the upload.
          error("Error uploading data.  TotalRowsSent was the same value for two sends [" + totalRowsSent + "]");
          serverAcceptingData = false;
          break;
        }
        previousRowsSent = totalRowsSent;
      }
      return result;
    } catch (SQLException e) {
      error("SQL problem with sending data", e);
      throw e;
    } finally {
      if (resultSetDataSource != null) {
        resultSetDataSource.close();
      }
    }
  }

  /**
   * Reads data from the input stream and sends it to the server.  Returns true iff all data is completely sent.
   * The input file must be a CSV file.
   *
   * @param name   a name for the "query" for which the data in the file are the results.
   * @param source an input stream for a CSV file holding the results
   * @return true if the data is successfully sent, false if not.
   */
  private boolean send(String name, InputStream source) {
    // If the input stream is null, there's no data to send and we're done.  Possibly we should
    // indicate an error, since this doesn't seem like a case that should happen.
    if (source == null) return true;

    RowDataSource dataSource;
    try {
      info("Creating CSVFileDataSource");
      dataSource = new CSVFileDataSource(name, source, _dataExtractor);
      rowsForCurrentTable = 0;
    } catch (IOException e) {
      error("Error reading file data source", e, false);
      return false;
    }

    // sendPostRequestToServer() has a maximum limit on the number of rows it will send, even if the sends are
    // successful.  If the file is larger than that maximum, loop repeatedly until all data has
    // been sent.
    info("Sending data for [" + name + "]");
    boolean needToRerun = true;
    int iterations = 0;
    while (needToRerun && iterations++ < MAX_ITERATIONS_FOR_RUNNING && continueSendingData()) {
      needToRerun = false;
      if (wasNotAbleToSendEntireResultSet(sendData(dataSource, UploadType.INITIAL_CSV))) {
        needToRerun = true;
        info("Continuing to send data for [" + name + "] - total rows sent: " + totalRowsSent);
      } else {
        info("Completed sending data for [" + name + "] - total rows sent: " + totalRowsSent);
      }
    }
    return true;
  }

  /**
   * Sends data from the passed in source to the server via http.  Returns true iff the send is successful
   * and the entire data set was sent, i.e. was the end of the data set reached and successfully sent.
   * <p/>
   * Note that there is a maximum amount of data that this method will
   * send, depending on DEFAULT_MAX_ATTEMPTS_TO_SEND_RESULTS amd getMaxRowsToSend().  If the data set is
   * larger than the maximum, the method should be called multiple times until all the data is sent.  The
   * RowDataSource object is expected to keep track of how far through the data set the sending process
   * has proceeded.
   *
   * @param dataSource the set of data to send
   * @param uploadType which type of data upload are we doing: incremental, initial csv file, or customer csv file.
   * @return true if the entire set of data was sent, false if either sending failed or we didn't send the
   *         entire data set.
   */
  // todo reconcile this with the sendData that operates on a key/value pair.
  private boolean sendData(RowDataSource dataSource, UploadType uploadType) {
    GsonBuilder gsonBuilder = new GsonBuilder();
    Gson gson = gsonBuilder.create();

    int attempts = 0;
    QueryResult data = null;
    while (attempts++ < DEFAULT_MAX_ATTEMPTS_TO_SEND_RESULTS && continueSendingData()) {
      try {
        data = buildResultPackage(dataSource);
      } catch (SQLException e) {
        error("Error reading from database", e);
        return false;
      } catch (ParseException e) {
        error("Error reading parsing data", e);
        return false;
      } catch (IOException e) {
        error("Error reading data source", e);
        return false;
      }
      if (data.getRowCount() == 0) return true;

      HttpMethod response;
      try {
        response = sendPostRequestToServer(createQueryResultPayload(data), uploadType);
      } catch (Exception e) {
        error("Error sending data to server for table [" + dataSource.getName() + "]", e);
        response = null;
      }
      if (response != null && response.getStatusCode() == HttpStatus.SC_OK) {
        UploadAcknowledge acknowledgement = null;
        String responseBody;
        try {
          responseBody = restoreDoubleQuotes(response.getResponseBodyAsString());
          acknowledgement = gson.fromJson(responseBody, UploadAcknowledge.class);
        } catch (IOException e) {
          error("Error getting response body", e);
        }
        if (acknowledgement != null) {
          if (acknowledgement.getRowsUploaded() != data.getRowCount()) {
            // We have a problem - the server thinks we sent a different number of row than the data object
            // believes it has.
            // TODO: What should we do?  Continue to send more data?  Try to resend the data?  If resend, we need
            // TODO: some way to back out whatever data got written on the server in the previous send.
            // TODO: We could simply notify the server about the problem and let it record it so we can look
            // TODO: it later.  Not clear what's best.
            error("Error in data transfer - data sent had [" + data.getRowCount() + "] rows, data received [" + acknowledgement.getRowsUploaded() + "] rows");
          }
          totalRowsSent += acknowledgement.getRowsUploaded();
          rowsForCurrentTable += acknowledgement.getRowsUploaded();
          querySummary.addQueryInfo(dataSource.getName(), acknowledgement.getRowsUploaded());
        }

        if (!data.wasCutShort()) {
          info("Completed sending data for [" + dataSource.getName() + "] - rows for table [" + rowsForCurrentTable + "] total rows sent: " + totalRowsSent);
          if (dataSource.expectedRowsKnown()) {
            // Check to see if we got the number of rows we expected.
            if (rowsForCurrentTable != dataSource.getExpectedRows()) {
              error("Data for [" + dataSource.getName() + "] may be corrupt.  Expected [" + dataSource.getExpectedRows() + "] rows, but got [" + rowsForCurrentTable + "]");
            }
          }
          return true;
        }

        info("Continuing to send data for [" + dataSource.getName() + "] - rows for table [" + rowsForCurrentTable + "] total rows sent: " + totalRowsSent);
        // If the send was successful, but there is more data, continue around the loop and send
        // more data.
      } else if (response != null && response.getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
        UploadAcknowledge acknowledgement = null;
        String responseBody;
        try {
          responseBody = restoreDoubleQuotes(response.getResponseBodyAsString());
          acknowledgement = gson.fromJson(responseBody, UploadAcknowledge.class);
        } catch (IOException e) {
          error("Error getting response body", e);
        }
        if (acknowledgement != null) {
          if (UPLOAD_DISABLED.equals(acknowledgement.getMessage())) {
            info(UPLOAD_DISABLED);
            serverAcceptingData = false;
            return true;
          }
        }
      } else {
        String message;
        if (response == null) {
          message = "Error sending data to server";
        } else {
          message = "Received server error code: " + response.getStatusCode() + " " + response.getStatusText();
        }
        _dataExtractor.getDataExtractorLog().error("Exiting.  " + message);
        throw new DataExtractorServerException(message);
      }
    }

    if (data != null && !data.wasCutShort())
      return true;
    else
      return false;
  }

  /**
   * Sends data from the passed in source to the server via http.  Returns true iff the send is successful
   * and the entire data set was sent, i.e. was the end of the data set reached and successfully sent.
   * <p/>
   * Note that there is a maximum amount of data that this method will
   * send, depending on DEFAULT_MAX_ATTEMPTS_TO_SEND_RESULTS amd getMaxRowsToSend().  If the data set is
   * larger than the maximum, the method should be called multiple times until all the data is sent.  The
   * RowDataSource object is expected to keep track of how far through the data set the sending process
   * has proceeded.
   *
   * @param data the set of data to send
   * @param uploadType which type of data upload are we doing: incremental, initial csv file, or customer csv file.
   * @return true if the entire set of data was sent, false if either sending failed or we didn't send the
   *         entire data set.
   */
  // todo reconcile this with the sendData that operates on a row source data type.
  // This method is here because it requires different error checking  than the row source  version. We also need to send data that is not
  // a row source type. The row source method has error handling that is so specific to row source types that it didn't seem worthwhile to try
  // to generify that method. So we have this rather ugly copy/paste version.
  private boolean sendData(NameValuePair[] data, int numRowsInData, UploadType uploadType) {
    GsonBuilder gsonBuilder = new GsonBuilder();
    Gson gson = gsonBuilder.create();

    int attempts = 0;
    while (attempts++ < DEFAULT_MAX_ATTEMPTS_TO_SEND_RESULTS && continueSendingData()) {

      HttpMethod response;
      try {
        response = sendPostRequestToServer(data, uploadType);
      } catch (Exception e) {
        error("Error sending data to server for data [" + data[0] + "]", e);
        response = null;
      }
      if (response != null && response.getStatusCode() == HttpStatus.SC_OK) {
        UploadAcknowledge acknowledgement = null;
        String responseBody;
        try {
          responseBody = restoreDoubleQuotes(response.getResponseBodyAsString());
          acknowledgement = gson.fromJson(responseBody, UploadAcknowledge.class);
        } catch (IOException e) {
          error("Error getting response body", e);
        }
        if (acknowledgement != null) {
          totalRowsSent += acknowledgement.getRowsUploaded();
          // if all of the data has been sent successfully, return now otherwise retry that data
          if (acknowledgement.getRowsUploaded() != numRowsInData) {
            error("Data for [" + data[0] + "] may be corrupt.  Expected [" + numRowsInData + "] rows, but got [" + acknowledgement.getRowsUploaded() + "]");
          }
        }
        return true;
      } else if (response != null && response.getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
        UploadAcknowledge acknowledgement = null;
        String responseBody;
        try {
          responseBody = restoreDoubleQuotes(response.getResponseBodyAsString());
          acknowledgement = gson.fromJson(responseBody, UploadAcknowledge.class);
        } catch (IOException e) {
          error("Error getting response body", e);
        }
        if (acknowledgement != null) {
          if (UPLOAD_DISABLED.equals(acknowledgement.getMessage())) {
            info(UPLOAD_DISABLED);
            serverAcceptingData = false;
            return true;
          }
        }
      } else {
        String message;
        if (response == null) {
          message = "Error sending data to server";
        } else {
          message = "Received server error code: " + response.getStatusCode() + " " + response.getStatusText();
        }
        _dataExtractor.getDataExtractorLog().error("Exiting.  " + message);
        throw new DataExtractorServerException(message);
      }
    }
    return true;
  }

  private String restoreDoubleQuotes(String response) {
    return response.replaceAll("&#034;", "\"");
  }

  private StringBuilder constructRawRow(QueryResult.ResultRow resultRow) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String value : resultRow.getResults()) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      if (value.length() < 20) {
        sb.append(value);
      } else {
        sb.append(value.substring(0,20) + "...");
      }
    }
    return sb;
  }

  public RowDataSource getResultsFromFile(String name, InputStream stream) throws IOException {
    RowDataSource dataSource = new CSVFileDataSource(name, stream, _dataExtractor);
    return dataSource;
  }

  protected String getGwAuth() {
    if (gwAuth == null) {
      if (_dataExtractor.getGwAuth() != null) {
        gwAuth = _dataExtractor.getGwAuth();
      }
      else {
        // Like the main HttpClient object, we cache the gwAuth token.  But in theory at least, we should only
        // need it once.  We use a different HttpClient to connect to the login service (CAS) to get the auth token.
        HttpClient httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());
        addProxyInfo(httpClient);
        String url = "https://" + _dataExtractor.getCasHost() + "/login";
        PostMethod post = createAuthPostRequest(url);

        try {
          httpClient.executeMethod(post);
          // The login service seems to always return SC_MOVED_TEMPORARILY, I think so that browser logins will get
          // redirected back to where they want to go or something.  In any case, that indicates success as far as
          // we're concerned.
          if (post.getStatusCode() != HttpStatus.SC_OK && post.getStatusCode() != HttpStatus.SC_MOVED_TEMPORARILY) {
            int statusCode = post.getStatusCode();
            _dataExtractor.getDataExtractorLog().error("Error getting authorization: " + statusCode);
          } else {
            Cookie[] cookies = httpClient.getState().getCookies();
            for (Cookie c : cookies) {
              if (c.getName().equals("gwAuth")) {
                gwAuth = c.getValue();
                break;
              }
            }
          }
        } catch (IOException e) {
          _dataExtractor.getDataExtractorLog().error("Error getting authorization");
          _dataExtractor.getDataExtractorLog().error(e);
        }
      }
    }

    return gwAuth;
  }


  private PostMethod createAuthPostRequest(String url) {
    PostMethod method = new PostMethod(url);
    NameValuePair[] params = new NameValuePair[2];
    params[0] = new NameValuePair("u", getUsername());
    params[1] = new NameValuePair("p", getCompanyPassword());
    method.addParameters(params);
    return method;
  }

  private HttpMethod sendPostRequestToServer(NameValuePair[] payload, UploadType uploadType) {
    int command = uploadType.getCommand();
    PostMethod upload;
    if (command == CUSTOMER_FILEDEF_UPLOAD) {
      upload = createCSVFileDefUploadPostRequest(command);
    } else if (command == CUSTOMER_CSV_UPLOAD) {
      upload = createCSVDataUploadPostRequest(command);
    } else {
      upload = createDataPostRequest(command);
    }
    upload.setRequestBody(payload);
    sendRequest(upload);
    return upload;
  }

  public HttpMethod sendException(String message, Exception e) {
    try {
      PostMethod upload = createExceptionPostRequest(ERROR_UPLOAD);
      upload.setRequestBody(createExceptionPayload(message, e));
      sendRequest(upload);
      return upload;
    } catch (Exception anotherExceptionThrown) {
      // Note that we call error with sendToServer false, so that it doesn't
      // attempt to send the error again if it failed.
      error("Error uploading exception", anotherExceptionThrown, false);
      return null;
    }
  }

  public void sendLogs(int numToSend) {
    List<File> logs = findLogs(numToSend);
    if (logs != null) {
      for (File log : logs) {
        info("uploading log [" + log.getAbsolutePath() + "]");
        sendLogs(log);
      }
    }
  }

  private List<File> findLogs(int maxDaysBack) {
    return _dataExtractor.getDataExtractorLog().findLogs(maxDaysBack);
  }

  public HttpMethod sendLogs(InputStream stream) {
    PostMethod upload = createLogsPostRequest(LOG_UPLOAD);
    try {
      upload.setRequestBody(createLogsPayload(stream));
      sendRequest(upload);
      return upload;
    } catch (Exception e) {
      error("Error sending log file", e);
      return null;
    }
    finally {
      upload.releaseConnection();
    }
  }

  private void sendLogs(File file) {
    try {
      sendLogs(new FileInputStream(file));
    } catch (FileNotFoundException e) {
      error("Error sending log file", e);
    }
  }

  private NameValuePair[] createQueryResultPayload(QueryResult queryResult) {
    NameValuePair[] result = new NameValuePair[1];
    Gson gson = new Gson();
    String queryResultGson = gson.toJson(queryResult);
    // By default, data sent over html is encoded with ISO-8859-1.  Unfortunately, that charset
    // doesn't include Japanese or any of the Chinese character sets, plus numerous others.  To
    // handle those characters, we need to use UTF-8.  So, after we turn the data into Json, we
    // will urlencode it.
    String encoded;
    try {
      encoded = URLEncoder.encode(queryResultGson, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      error("Error constructing query result payload: " + e.getLocalizedMessage());
      return null;
    }
    result[0] = new NameValuePair("results", encoded);
    return result;
  }

  private NameValuePair[] createQuerySummaryPayload(QueryConnectionSummary querySummary) {
    NameValuePair[] result = new NameValuePair[1];
    Gson gson = new Gson();
    result[0] = new NameValuePair("results", gson.toJson(querySummary));
    return result;
  }

  private NameValuePair[] createExceptionPayload(String message, Exception e) {
    NameValuePair[] result = new NameValuePair[1];
    Gson gson = new Gson();
    ExtractionException exception = new ExtractionException(message, e);
    result[0] = new NameValuePair("exception", gson.toJson(exception));
    return result;
  }

  private NameValuePair[] createLogsPayload(InputStream logFile) {
    NameValuePair[] result = new NameValuePair[1];
    Gson gson = new Gson();
    ExtractionLog logs = new ExtractionLog(logFile);
    logs.trimToMaxSize(MAX_CHARACTERS_TO_SEND);
    result[0] = new NameValuePair("logs", gson.toJson(logs));
    return result;
  }

  private NameValuePair[] createTestToolResultsPayload(TestToolResults results) {
    NameValuePair[] result = new NameValuePair[1];
    Gson gson = new Gson();
    result[0] = new NameValuePair("results", gson.toJson(results));
    return result;
  }

  private TestToolCommands unpackTestToolCommands(InputStream stream) throws IOException {
    Gson gson = new Gson();
    TestToolCommands commands = gson.fromJson(new InputStreamReader(stream), TestToolCommands.class);
    stream.close();
    return commands;
  }

  private DataExtractor.QueryRun runQuery(QueryDefinition queryDefinition) throws SQLException {
    Long checksum = getChecksum(queryDefinition);
    if (checksumIndicatesDataHasntChanged(queryDefinition, checksum)) {
      info("Checksum matches for " + queryDefinition.getName());
      return null;
    }
    PreparedStatement statement = _dataExtractor.createPreparedStatement(queryDefinition);
    return _dataExtractor.runAQuery(statement);
  }

  private boolean checksumIndicatesDataHasntChanged(QueryDefinition queryDefinition, Long checksum) {
    if (queryDefinition.getLastChecksum() == null || checksum == null || checksum == 0) return false;

    return queryDefinition.getLastChecksum().equals(checksum);
  }

  private boolean canCalculateChecksum(QueryDefinition queryDefinition) {
    return queryDefinition.getChecksumSQL() != null &&
            queryDefinition.getChecksumSQL().length() > 5;
  }

  private Long getChecksum(QueryDefinition queryDefinition) {
    if (!canCalculateChecksum(queryDefinition)) return null;

    try {
      PreparedStatement checksumSQL = _dataExtractor.createChecksumPreparedStatement(queryDefinition);
      DataExtractor.QueryRun run = _dataExtractor.runAQuery(checksumSQL);
      ResultSet resultSet = run.getResultSet();
      if (resultSet.next()) {
        return resultSet.getLong(1);
      }
    } catch (SQLException sqlException) {
      error("Error getting checksum: " + queryDefinition.getChecksumSQL(_dataExtractor.getDBTag(), _dataExtractor.getDB()), sqlException);
    }
    return null;
  }


  private void setQueriesFor(boolean isTestConnection) throws IOException {
    info("Retrieving queries from [" + getDataExtractionServerURL() + "]");
    HttpMethod httpMethod = createQueryDefinitionGetRequest(
      isTestConnection ? QUERY_TEST_CONNECTION_REQUEST : QUERY_REQUEST);
    sendRequest(httpMethod);
    List<QueryDefinition> queryDefs = convertResponseBodyToQueryDefinitions(httpMethod.getResponseBodyAsStream());
    logReceipt(queryDefs);
    _dataExtractor.setQueries(queryDefs);
  }

  private void logReceipt(List<QueryDefinition> queryDefs) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < queryDefs.size(); i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(queryDefs.get(i).getName());
    }
    info("Retrieved [" + queryDefs.size() + "] queries: " + sb.toString());

    if (printFullQueries) {
      for (QueryDefinition qDef : queryDefs) {
        try {
          _dataExtractor.getDataExtractorLog().display("query name [" + qDef.getName() + "] latest update date [" + qDef.getLatestUpdateDate() + "]", false);
          String query = _dataExtractor.getPreparedStatementQuery(qDef);
          _dataExtractor.getDataExtractorLog().display(query, false);
        } catch (Throwable e) {
          error("Error getting sql for query [" + qDef.getName() + "] message [" + e.getLocalizedMessage() + "]");
        }
      }
    }
  }

  private void logReceiptOfFileDefs(List<CSVFileDefinition> fileDefs) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fileDefs.size(); i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(fileDefs.get(i).getName());
    }
    info("Retrieved [" + fileDefs.size() + "] queries: " + sb.toString());

    if (printFullQueries) {
      for (CSVFileDefinition fileDefinition : fileDefs) {
        _dataExtractor.getDataExtractorLog().display("filedefinition name [" + fileDefinition.getName() + "]" , false);
        _dataExtractor.getDataExtractorLog().display(fileDefinition.getAsXML(), false);
      }
    }
  }


  private void setDatabaseConnectionInfo() {
    if (sendDatabaseConnectionInfo()) {
      setQueryDerivedParameter(DB_CONNECTION_URL, _dataExtractor.getDBURL());
      setQueryDerivedParameter(DB_CONNECTION_USER, _dataExtractor.getDBUserID());
      setQueryDerivedParameter(DB_CONNECTION_DB, _dataExtractor.getDB());
      setQueryDerivedParameter(DB_CONNECTION_SCHEMA, _dataExtractor.getDBTag());
    }
  }

  private void setQueryDerivedParameter(String name, String string) {
    queryDerivedParameters.put(name, string);
  }

  private void setCustomerCSVFileParameter(String value) {
    setQueryDerivedParameter(CUSTOMER_CSV_FILETYPE, value);
  }

  private List<QueryDefinition> convertResponseBodyToQueryDefinitions(InputStream stream) throws IOException {
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(ColumnDef.class, ColumnDef.createJSONDeserializer());
    Gson gson = gsonBuilder.create();

    QueryDefinitionJSON vals = gson.fromJson(new InputStreamReader(stream), QueryDefinitionJSON.class);
    stream.close();
    if (vals == null) {
      return new ArrayList<>();
    } else {
      return vals.getQueries();
    }
  }

  private void addProxyInfo(HttpClient client) {
    // If we need to use a proxy, set the client up to use one
    int proxyPort = 8080;
    if (_dataExtractor.getProxyHost() != null) {
      if (_dataExtractor.getProxyPort() != null) {
        proxyPort = Integer.parseInt(_dataExtractor.getProxyPort());
      }
      client.getHostConfiguration().setProxy(_dataExtractor.getProxyHost(), proxyPort);
      _dataExtractor.getDataExtractorLog().info("connecting with proxy [" + _dataExtractor.getProxyHost() + "] on port [" + proxyPort + "]");

      AuthScope scope;
      switch (_dataExtractor.getProxyAuthenticationSchema()) {
        case DataExtractor.NO_PROXY_AUTH:
          _dataExtractor.getDataExtractorLog().info("Accessing proxy with no authentication");
          break;
        case DataExtractor.PROXY_AUTH_BASIC:
          _dataExtractor.getDataExtractorLog().info("Accessing proxy with Basic authentication");
          UsernamePasswordCredentials proxyUserPasswordCred;
          // Check for Windows usage
          int domainIndex =  _dataExtractor.getProxyUsername().indexOf('\\');
          if (domainIndex != -1) {
            // Split the username into domain and user
            String domain = _dataExtractor.getProxyUsername().substring(0, domainIndex);
            String username = _dataExtractor.getProxyUsername().substring(domainIndex+1);
            _dataExtractor.getDataExtractorLog().info("creating credentials for domain [" + domain + "] user [" + username + "]");
            proxyUserPasswordCred = new NTCredentials(username, _dataExtractor.getProxyPassword(), "", domain);
          } else {
            proxyUserPasswordCred = new UsernamePasswordCredentials(_dataExtractor.getProxyUsername(), _dataExtractor.getProxyPassword());
          }
          // Note that we do not call client.getState().setCredentials().  I believe that's the credentials for the final
          // connection, not the proxy, and it is handled elsewhere.
          scope = new AuthScope(_dataExtractor.getProxyHost(), proxyPort);
          client.getState().setProxyCredentials(scope, proxyUserPasswordCred);
          // The call to setDoAuthentication() seems to be necessary.  Without it we get errors like
          // Bad status code 407 Proxy Authentication Required
          //
          // We still get warnings, though:
          // Required credentials not available for BASIC <any realm>@cbs.guidewire.com:443
          // Preemptive authentication requested but no default credentials available
          // If I understand correctly, these are because we use credentials for the proxy, but not for
          // cbs.guidewire.com, and once we have starting using authentication (possibly by setting
          // setAuthenticationPreemptive to true), the code expects to continue using authentication.
//            request.setDoAuthentication(true); // Not sure if we need this now that we are using gwAuth
          client.getParams().setAuthenticationPreemptive(true);
          break;
        case DataExtractor.PROXY_AUTH_NTLM:
          _dataExtractor.getDataExtractorLog().info("Accessing proxy with NTLM authentication");
          NTCredentials credentials = new NTCredentials(_dataExtractor.getProxyUsername(), _dataExtractor.getProxyPassword(), "", _dataExtractor.getProxyDomain());
          scope = new AuthScope(_dataExtractor.getProxyHost(), proxyPort);
          client.getState().setProxyCredentials(scope, credentials);
          client.getParams().setAuthenticationPreemptive(true);
          break;
        default:
          _dataExtractor.getDataExtractorLog().info("Invalid proxy authentication schema [" + _dataExtractor.getProxyAuthenticationSchema() + "]");
          break;
      }
    }
  }

  private HttpClient getClient() {
    // We create a new client the first time we connect, and then after that we reuse it.  This will preserve any
    // session cookies from the first connection.
    if (desClient == null) {
      desClient = new HttpClient(new MultiThreadedHttpConnectionManager());

//      // Try to minimize the amount of junk that httpClient writes to the log
      Logger.getLogger("org.apache.http.wire").setLevel(Level.FINEST);
      Logger.getLogger("org.apache.http.headers").setLevel(Level.FINEST);
      Logger.getLogger("org.apache.commons.httpclient").setLevel(Level.FINEST);
      Logger.getLogger("global").setLevel(Level.FINEST);

      addProxyInfo(desClient);

      // Now set up the auth cookie
//      String authToken = getGwAuth();
//      Cookie cookie = new Cookie(_dataExtractor.getDomain(), "gwAuth", authToken);
//      cookie.setPath("/");
//      desClient.getState().addCookie(cookie);
    }

    return desClient;
  }


  private String getAccessTokenFromOktaHub() throws Exception {
    return DESAuthorizer.getAccessTokenFromOktaHub("scp.data_extraction_service.user",
            _dataExtractor.getOktaClientId(), _dataExtractor.getOktaClientSecret(),
            _dataExtractor.getOktaHost()).getAccessToken();
  }


  private void sendRequest(final HttpMethod request) {
    HttpClient client = getClient();

    DataExtractorLog log = _dataExtractor.getDataExtractorLog();
    try {
      if (_dataExtractor.getOktaClientToken() == null) {
        try {
          String authToken = getAccessTokenFromOktaHub();
          _dataExtractor.setOktaClientToken(authToken);
        } catch (Exception e) {
          log.errorToLogMsgToConsole("Error: Unable to authenticate.", ExceptionUtils.getStackTrace(e));
          throw new InvalidResponseException("Error: Unable to authenticate.");
        }
      }
      request.addRequestHeader("Authorization", "Bearer " + _dataExtractor.getOktaClientToken());
      client.executeMethod(request);
      int statusCode = request.getStatusCode();

      // We don't give an error for SC_SERVICE_UNAVAILABLE - that can indicate that the server is not accepting
      // requests at the moment, and will be handled higher up
      if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_SERVICE_UNAVAILABLE) {
        unsuccessfulConnectAttempt = true;
        String statusMessage = request.getStatusText() + ": " + statusCode;
        String desResponse = "DES response: " + request.getResponseBodyAsString();

        if (statusCode == HttpStatus.SC_REQUEST_TOO_LONG) {
          NameValuePair results = ((PostMethod)request).getParameter("results");
          int size = -1;
          if (results != null) {
            String paramString = results.getValue();
            if (paramString != null) {
              size = paramString.length();
            }
          }
          statusMessage += " payload size [" + size + "]";
        }

        log.errorToLogMsgToConsole(statusMessage, statusMessage);
        log.errorToLogMsgToConsole(desResponse, desResponse);

        String exceptionMessage = statusMessage + " " + desResponse;
        throw new InvalidResponseException(exceptionMessage);
      }

      // see if authentication failed
      if (request.getURI().getURI().contains(_dataExtractor.getCasHost())) {
        String message = "Authentication with DES failed.";
        log.errorToLogMsgToConsole(message, message);
        throw new InvalidResponseException(message);
      }

    }

    catch (SSLHandshakeException she) {
      log.errorToLogMsgToConsole(she, "Could not connect to Guidewire server.  Please check to make sure that the IP address you’re connecting from is whitelisted with Guidewire, " +
        "and also to make sure that your firewalls allow connections to " + getDataExtractionServerURL());
      unsuccessfulConnectAttempt = true;
      throw new CantAccessQueryInfoException(she);
    }

    catch (IOException e) {
      if (_dataExtractor.getProxyHost() == null) {
        if (e instanceof ConnectException || e instanceof UnknownHostException) {
          log.errorToLogMsgToConsole(e, "Could not connect to Guidewire server.  Please check with your IT department to see if you need to use a proxy server and then contact Guidewire Support.");
        } else {
          log.errorToLogMsgToConsole(e, "Could not connect to Guidewire server.  Please contact Guidewire Support.");
        }
      } else if (e instanceof ConnectException || e instanceof UnknownHostException) {
        int proxyPort = 8080;
        if (_dataExtractor.getProxyPort() != null) {
          proxyPort = Integer.parseInt(_dataExtractor.getProxyPort());
        }
        log.errorToLogMsgToConsole(e, "Could not connect to proxy server [" + _dataExtractor.getProxyHost() + "] on port [" + proxyPort + "].  Please check the settings and contact your IT department.");
      } else {
        int proxyPort = 8080;
        if (_dataExtractor.getProxyPort() != null) {
          proxyPort = Integer.parseInt(_dataExtractor.getProxyPort());
        }
        log.errorToLogMsgToConsole(e, "Could not connect to Guidewire server.  Please check your proxy server [" + _dataExtractor.getProxyHost() + "] and port [" + proxyPort + "] and contact Guidewire Support.");
      }
      unsuccessfulConnectAttempt = true;
      throw new CantAccessQueryInfoException(e);
    }
  }

  private HttpMethod createClientInfoRequest() {
    HttpMethod method = new GetMethod(getDataExtractionServerURL() + "/clientinfo.htm");
    setParameters(method, getParameters(null));
    return method;
  }

  private HttpMethod createQueryDefinitionGetRequest(Integer command) {
    HttpMethod method = new GetMethod(getDataExtractionServerURL() + "/query.htm");
    setParameters(method, getParameters(command));
    return method;
  }

  private HttpMethod createFileDefinitionGetRequest(Integer command) {
    HttpMethod method = new GetMethod(getDataExtractionServerURL() + "/csvFileDefinition.htm");
    setParameters(method, getParameters(command));
    return method;
  }

  private PostMethod createDataPostRequest(Integer command) {
    PostMethod method = new PostMethod(getDataExtractionServerURL() + "/upload.htm");
    setParameters(method, getParameters(command));
    return method;
  }

  private PostMethod createCSVFileDefUploadPostRequest(Integer command) {
    PostMethod method = new PostMethod(getDataExtractionServerURL() + "/admin/submitCSVFileDef.htm");
    setParameters(method, getParameters(command));
    return method;
  }

  private PostMethod createCSVDataUploadPostRequest(Integer command) {
    PostMethod method = new PostMethod(getDataExtractionServerURL() + "/upload/submitCSVFileUpload.htm");
    setParameters(method, getParameters(command));
    return method;
  }

  private PostMethod createExceptionPostRequest(Integer command) {
    PostMethod method = new PostMethod(getDataExtractionServerURL() + "/error.htm");
    setParameters(method, getParameters(command));
    return method;
  }

  private PostMethod createLogsPostRequest(Integer command) {
    PostMethod method = new PostMethod(getDataExtractionServerURL() + "/logs.htm");
    setParameters(method, getParameters(command));
    return method;
  }

  private PostMethod createQuerySummaryPostRequest(UploadType uploadType) {
    PostMethod method = new PostMethod(getDataExtractionServerURL() + "/upload.htm");
    if (uploadType.equals(UploadType.CUSTOMER_CSV)) {
      method = new PostMethod(getDataExtractionServerURL()  + "/upload/submitCSVFileUpload.htm");
      setParameters(method, getParameters(CUSTOMER_SUMMARY_CSV));
    } else if (uploadType.equals(UploadType.INITIAL_CSV)) {
      setParameters(method, getParameters(QUERY_SUMMARY_CSV));
    } else {
      setParameters(method, getParameters(QUERY_SUMMARY));
    }
    return method;
  }


  private HttpMethod createTestToolGetRequest(Integer command, boolean isWindows) {
    HttpMethod method = new GetMethod(getDataExtractionServerURL() + "/query.htm");
    Collection<NameValuePair> params =  getParameters(command);
    params.add(new NameValuePair(IS_WINDOWS_PARAMETER, isWindows ? "true" : "false" ));
    setParameters(method, params);
    return method;
  }

  private HttpMethod createCSVCheckGetRequest(Integer command) {
    HttpMethod method = new GetMethod(getDataExtractionServerURL() + "/query.htm");
    setParameters(method, getParameters(command));
    return method;
  }


  void setParameters(HttpMethod method, Collection<NameValuePair> parameters) {
    ArrayList<NameValuePair> parameterList = new ArrayList<>(parameters);

    // add client if we have one
    String client = _dataExtractor.getClient();
    if (client != null) {
      NameValuePair clientNameValuePair = new NameValuePair("client", client);
      parameterList.add(clientNameValuePair);
    }

    NameValuePair[] nameValuePairs = new NameValuePair[parameterList.size()];
    parameterList.toArray(nameValuePairs);

    method.setQueryString(nameValuePairs);
  }


  private Collection<NameValuePair> getParameters(Integer command) {
    List<NameValuePair> result = new ArrayList<>();
//    result.add(new NameValuePair("username", getUsername()));
//    result.add(new NameValuePair("password", getCompanyPassword()));
    result.add(new NameValuePair("version", DataExtractionRunner.PROTOCOL_VERSION.toString()));
    if (command != null) {
      result.add(new NameValuePair("command", command.toString()));
    }
    for (String arg : queryDerivedParameters.keySet()) {
      result.add(new NameValuePair(arg, queryDerivedParameters.get(arg)));
    }
    return result;
  }

  // This is thrown when the server responds with something other than 400
  private class InvalidResponseException extends RuntimeException {
    private InvalidResponseException(String message) {
      super(message);
    }
  }

  public class CantAccessQueryInfoException extends RuntimeException {
    private CantAccessQueryInfoException(Exception e) {
      super(e);
    }
  }

  private void info(String message) {
    _dataExtractor.getDataExtractorLog().info(message);
  }

  private void error(String message, Exception e) {
    error(message, e, true);
  }

  private void error(String message, Exception e, boolean sendToServer) {
    // Note that for authentication failures, we don't try to send the exception to the server,
    // since we won't be able to authenticate for that, either.  Also, the server will have already
    // logged that failure.
    if (sendToServer && !AUTHENTICATION_FAILED.equals(e.getMessage())) {
      sendException(message, e);
    }
    _dataExtractor.getDataExtractorLog().errorToLogMsgToConsole(e, message);
  }

  private void error(String message) {
    _dataExtractor.getDataExtractorLog().error(message);
  }

  private class DataExtractorServerException extends RuntimeException {
    private DataExtractorServerException(String message) {
      super(message);
    }
  }
}
