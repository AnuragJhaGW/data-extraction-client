package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.CustomerCSVFileDataSource;
import com.guidewire.cloudviewer.datamoving.TestToolCommands;
import com.guidewire.cloudviewer.datamoving.TestToolResults;
import com.guidewire.cloudviewer.datamoving.client.DataExtractorClient;
import com.guidewire.cloudviewer.datamoving.client.NaiveTrustProvider;
import com.guidewire.tools.DataExtractionUtils;
import com.guidewire.tools.MiscUtils;
import com.guidewire.tools.PropertiesUtils;
import org.apache.commons.cli.HelpFormatter;
import org.xml.sax.SAXException;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

import static com.guidewire.tools.benchmarking.DecCommandLine.*;


/**
 * DataExtractionRunner takes in the users commands, creates the necessary objects and performs the actions for the user.
 * To execute the commands, the DER will catch exceptions and reattempt in order to get around temporary
 * conditions (DB or web maintenance, server resets, etc.)
 */
public class DataExtractionRunner {

  // command line options
  private static final String DefaultConfigFile = "config.xml";


  // Used as the key phrase for encryption.
  public static final String PHRASE = "Another fine mess you've gotten us into, Ollie";


  /**
   * This int is the version of the protocol.  Any time any object that is being shipped between the client and the
   * server changes, this version must be bumped and the server code receiving the request must handle both the new
   * version and previous versions.
   */
  public static final Integer PROTOCOL_VERSION = 1;

  private final DecCommandLine _decCommandLine;
  private final DataExtractor _dataExtractor;
  private final Properties _properties;
  private final DataExtractorClient _dataExtractorClient;


  /**
   * Constructor
   */
  DataExtractionRunner(String[] args) throws Exception{
    _decCommandLine = new DecCommandLine(args);
    _properties = getProperties();
    _dataExtractor = createExtractor();
    _dataExtractorClient = createDataExtractorClient();
  }


  /**
   * Create a DataExtractor that is configured with the correct config file
   */
  private DataExtractor createExtractor() throws Exception {
    // read args (to get config file)
    DataExtractor extractor = new DataExtractor(getConfigProcessor(), "", PHRASE);

    // Log the command line
    extractor.getDataExtractorLog().info(_decCommandLine.getCommandLineString());

    if (_decCommandLine.hasOption(TOP)) {
      extractor.setTop();
    }

    if (_decCommandLine.hasOption(SINCE)) {
      extractor.setSince(_decCommandLine.getOptionValue(SINCE));
    }

    // If the config file has a username, you can't set the -client arg.
    // If the config file doesn't have a username, i.e. your using a gwAuth token,
    // you must set the -client arg.
    if (_decCommandLine.hasOption(CLIENT)) {
      if (extractor.getUsername() != null) {
        throw new RuntimeException("If the config file has a username, you can't set the -" +
          CLIENT + " command line argument. -" +
          CLIENT + "=" + _decCommandLine.getOptionValue(CLIENT));
      }
      extractor.setClient(_decCommandLine.getOptionValue(CLIENT));
    }
    else {
      if (extractor.getUsername() == null && extractor.getOktaClientId() == null) {
        throw new RuntimeException("If the config file has no username, you must set the -" +
          CLIENT + " command line argument.");
      }
    }

    extractor.setGuidewireClientVersion(_properties.getProperty(DataExtractor.CLIENT_VERSION_PROPERTY));

    return extractor;
  }


  DataExtractor getDataExtractor() {
    return _dataExtractor;
  }


  ConfigProcessor getConfigProcessor() {
    String configFilePath = _decCommandLine.hasOption(FILE) ?
      _decCommandLine.getOptionValue(FILE) : DefaultConfigFile;

    return new FileConfigProcessor(configFilePath);
  }


  private Properties getProperties() throws IOException {
    Properties properties;
    File defaultProperties = new File("config/extract.properties");
    if (defaultProperties.exists()) {
      properties = PropertiesUtils.getProperties(defaultProperties.getAbsolutePath());
    } else {
      properties = PropertiesUtils.getProperties(DataExtractionUtils.getInputStreamBasedOnFileName("extract.properties"));
    }
    return properties;
  }


  private DataExtractorClient createDataExtractorClient() {
    return new DataExtractorClient(_dataExtractor,
      _decCommandLine.getStartTime(),
      _decCommandLine.getMaxRunTimeMilli());
  }


  /////////////////////////////////             Execution
  
  /**
   * Function to generate a 128 bit key from the given password and iv
   * @param password
   * @param iv
   * @return Secret key
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeySpecException
   */
  public static SecretKey generateSecretKey(String password, byte [] iv) throws InvalidKeySpecException, NoSuchAlgorithmException {
    KeySpec spec = new PBEKeySpec(password.toCharArray(), iv, 65536, 128); // AES-128
    SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
    byte[] key = secretKeyFactory.generateSecret(spec).getEncoded();
    return new SecretKeySpec(key, "AES");
  }

  private int execute() throws Exception{

    if (_decCommandLine.hasOption(HELP)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("full_extract or incremental_update", _decCommandLine.getCommandLineOptions());
      return 0;
    }

    if (_decCommandLine.hasOption(SHOW_HIDDEN_OPTIONS)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("extract", _decCommandLine.getAllCommandLineOptions());
      return 0;
    }

    if (_decCommandLine.hasOption(ENCRYPT)) {
      String toEncrypt = _decCommandLine.getOptionValue(ENCRYPT);

      SecureRandom secureRandom = new SecureRandom();
      byte[] iv = new byte[12];
      secureRandom.nextBytes(iv);
      SecretKey secretKey = generateSecretKey(PHRASE, iv);
      Cipher aes = Cipher.getInstance("AES/GCM/NoPadding");
      GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv);
      aes.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
      byte[] dbPasswordBytes = toEncrypt.getBytes(StandardCharsets.UTF_8);
      byte[] encrypted = aes.doFinal(dbPasswordBytes);

      ByteBuffer byteBuffer = ByteBuffer.allocate(4 + iv.length + encrypted.length);
      byteBuffer.putInt(iv.length);
      byteBuffer.put(iv);
      byteBuffer.put(encrypted);
      System.out.println("The encrypted string is [" + Base64.getEncoder().encodeToString(byteBuffer.array()) + "]");
      return 0;
    }


    int exitCode = -1;
    boolean done = false;
    int retries = 0;

    int numberOfRetries = _decCommandLine.getNumberOfRetries();
    final long startTime = _decCommandLine.getStartTime();
    final long maxRunTime = _decCommandLine.getMaxRunTimeMilli();
    int retryIntervalMilli = _decCommandLine.getRetryIntervalMilli();
    int retryMultiplier = _decCommandLine.getRetryMultiplier();

    while (!done && retries++ < numberOfRetries) {
      System.out.println("Attempting to run commands.  Attempt: " + retries);

      DataExtractor.ExecuteResults result = execute(startTime, maxRunTime);
      done = result.complete;
      exitCode = result.returnCode;

      if (retries >= numberOfRetries) {
        System.out.println("Maximum number of retries has been reached [" + numberOfRetries + "].  Check log files for results.");
      } else if (!done) {
        if (maxRunTime > 0 && (startTime + maxRunTime < System.currentTimeMillis() + retryIntervalMilli)) {
          System.out.println("Maximum run time will be exceeded by waiting for another retry so halting after [" + retries + "] retries.");
          break;
        }
        Thread.sleep(retryIntervalMilli);
        retryIntervalMilli = retryIntervalMilli * retryMultiplier;
      }
    }

    return exitCode;

  }


  /**
   * The main code for executing the user request.  Creates a DataExtractor and runs the appropriate command depending
   * on the command line options specified by the user.  Returns false if there was an error and the operation should
   * be retried again after a pause.  Note that true will be returned for error conditions where the code knows that
   * a retry will not be successful (for example, if the command was called with an inappropriate set of options).
   *
   * @return true if the operation has completely executed and false if an error occurred and the operation should
   *         be retried.
   */
  private DataExtractor.ExecuteResults execute(final long startTime, final long maxRunTime) {
    try {
      boolean query = _decCommandLine.hasOption(QUERY);
      boolean csv = _decCommandLine.hasOption(CSV_FILE);

      if (_decCommandLine.hasOption(VERSION)) {
        return displayVersion(_properties);
      }

      // Check that we have a valid set of options
      if (!query && _decCommandLine.hasNoValidOptions()) {
        // todo what should this error message do about hidden options?
        _dataExtractor.getDataExtractorLog().error("Need to specify either -help -testConnection -onlineUpdate or -query");
        _dataExtractor.getDataExtractorLog().closeLogFile();
        DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
        er.complete = true;
        er.returnCode = 0;
        return er;
      }

      if (query && !_dataExtractor.isReadyToQuery()) {
        _dataExtractor.getDataExtractorLog().error("Need to specify database url, database name, user and password in order to run query");
        _dataExtractor.getDataExtractorLog().closeLogFile();
        DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
        er.complete = true;
        er.returnCode = 0;
        return er;
      }

      // at this point we know we're going to call DES, so log the client info
      _dataExtractorClient.logClientInfo();

      if (_decCommandLine.hasOption(TEST_CONNECTION)) {
        return doTestConnection();
      } else if (hasCustomerCSVOption()) {
        return handleCustomerCSVRequests();

      } else if (csv) {
        return doInitialCSVUpload();
      }

      // The rest of the operations depend on having a set of queries, so get them
      if (!_decCommandLine.hasOption(TEST_CONNECTION) && !_decCommandLine.hasOption(ON_LINE_UPDATE) && !_decCommandLine.hasOption(CUSTOMER_CSV)
        && !_decCommandLine.hasOption(CUSTOMER_CSV_VALIDATE) && !_decCommandLine.hasOption(CUSTOMER_CSV_FILE_DEF_UPLOAD)) {
        loadQueries();
      }

      if (maxRunTime > 0 && (startTime + maxRunTime < System.currentTimeMillis())) {
        _dataExtractor.getDataExtractorLog().info("Max runtime exceeded.  _startTime [" + startTime + "] _maxRunTime [" + maxRunTime + "]");
        DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
        er.complete = true;
        er.returnCode = 0;
        return er;
      }

      if (_decCommandLine.hasOption(ON_LINE_UPDATE)) {
        return doIncrementalUpload();
      }
      if (query) {
        return doInitialExtract(startTime, maxRunTime);
      }
    } catch (Exception e) {
      if (_dataExtractor != null) {
        _dataExtractor.getDataExtractorLog().errorToLogMsgToConsole(e, "Error running data extraction client.  Please check the logs and contact Guidewire support.");
        _dataExtractor.getDataExtractorLog().closeLogFile();
      } else {
        e.printStackTrace();
      }
      DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
      er.complete = false;
      er.returnCode = 5;
      return er;
    }

    int returnCode = 0;
    if (_dataExtractor.getDataExtractorLog().hasErrorMessages()) {
      returnCode = 3;
      _dataExtractor.getDataExtractorLog().displayToStdOut("The following errors occurred during execution.  Please check the log file for more details.");
      for (String error : _dataExtractor.getDataExtractorLog().getErrorMessages()) {
        _dataExtractor.getDataExtractorLog().displayToStdOut("ERROR: " + error);
      }
    }
    _dataExtractor.getDataExtractorLog().closeLogFile();
    DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
    er.complete = true;
    er.returnCode = returnCode;
    return er;
  }


  private DataExtractor.ExecuteResults displayVersion(Properties properties) {
    for (Object o : properties.keySet()) {
      System.out.println("\n" + o + " : " + properties.get(o));
    }
    DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
    er.complete = true;
    er.returnCode = 0;
    return er;
  }


  private DataExtractor.ExecuteResults doInitialCSVUpload() throws SQLException, IOException {
    if (_decCommandLine.hasOption(TEST_MODE_ONLY)) {
      NaiveTrustProvider.setAlwaysTrust(true);
    }
    _dataExtractor.getDataExtractorLog().info("Loading from csv");
    String csvFile = _decCommandLine.getOptionValue(CSV_FILE);
    String name = _decCommandLine.getOptionValue(NAME);
    if (csvFile.endsWith(".csv")) {
      if (name == null || "".equals(name)) {
        _dataExtractor.getDataExtractorLog().error("If a single csv file is uploaded, you must supply the -name parameter");
        _dataExtractor.getDataExtractorLog().closeLogFile();
        DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
        er.complete = true;
        er.returnCode = 1;
        return er;
      }
      File f = new File(csvFile);
      if (!sendInitialCsvFile(f, name)) {
        _dataExtractor.getDataExtractorLog().closeLogFile();
        _dataExtractorClient.sendSummary(DataExtractorClient.UploadType.INITIAL_CSV);
        DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
        er.complete = true;
        er.returnCode = 0;
        return er;
      }
      _dataExtractorClient.sendSummary(DataExtractorClient.UploadType.INITIAL_CSV);
      _dataExtractorClient.clearConnectionSummary();
    } else {
      // Look for a set of .csv files that start with the prefix in csvFile
      String separator = System.getProperty("file.separator");
      String fullPath = "";
      if (csvFile.startsWith(separator) == false) {
        File currentDir = new File(".");
        fullPath = currentDir.getAbsolutePath() + separator;
      }
      int indexOfLastSeparator = csvFile.lastIndexOf(separator);
      if (indexOfLastSeparator == -1) {
        _dataExtractor.getDataExtractorLog().error("Could not find " + separator + " in file path [" + csvFile + "]");
        _dataExtractor.getDataExtractorLog().closeLogFile();
        DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
        er.complete = false;
        er.returnCode = 1;
        return er;
      }
      fullPath += csvFile.substring(0, indexOfLastSeparator);
      final String filePrefix = csvFile.substring(indexOfLastSeparator + 1);
      File directory = new File(fullPath);
      FilenameFilter csvFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name.startsWith(filePrefix) && name.endsWith(".csv")) {
            return true;
          }
          return false;
        }
      };

      File[] csvFiles = directory.listFiles(csvFilter);
      if (csvFiles == null || csvFiles.length == 0) {
        _dataExtractor.getDataExtractorLog().error("Couldn't find any files to upload from [" + fullPath + "]");
        _dataExtractor.getDataExtractorLog().closeLogFile();
        DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
        er.complete = true;
        er.returnCode = 1;
        return er;
      }

      for (File f : csvFiles) {
        // Get the table name out of the file name.  We assume that the table name starts after
        // the first _ and runs until the .csv in the name.  So for a file amica_foo_bar.csv, the
        // table name is foo_bar.
        String fileName = f.getName();
        int indexOfUnderscore = fileName.indexOf('_');
        String tableName = fileName.substring(indexOfUnderscore + 1, fileName.length() - 4);
        if (!sendInitialCsvFile(f, tableName)) {
          if (_dataExtractor.getDataExtractorLog().hasErrorMessages()) {
            _dataExtractor.getDataExtractorLog().displayToStdOut("The following errors occurred during execution.  Please check the log file for more details.");
            for (String error : _dataExtractor.getDataExtractorLog().getErrorMessages()) {
              _dataExtractor.getDataExtractorLog().displayToStdOut(error);
            }
          }
          _dataExtractor.getDataExtractorLog().closeLogFile();
          _dataExtractorClient.sendSummary(DataExtractorClient.UploadType.INITIAL_CSV);
          DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
          er.complete = true;
          er.returnCode = 1;
          return er;
        }
      }

      // Send the final summary.
      _dataExtractorClient.sendSummary(DataExtractorClient.UploadType.INITIAL_CSV);
    }

    DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
    er.complete = true;
    er.returnCode = 0;
    return er;
  }


  private DataExtractor.ExecuteResults createExecuteResults(int returnCode) {
    DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
    er.complete = true;
    er.returnCode = returnCode;
    return er;
  }

  private DataExtractor.ExecuteResults doIncrementalUpload() throws SQLException, IOException {
    if (_decCommandLine.hasOption(TEST_MODE_ONLY)) {
      NaiveTrustProvider.setAlwaysTrust(true);
    }
    if (_decCommandLine.hasOption(NO_DATABASE_CONNECTION_INFO)) {
      _dataExtractorClient.setSendDatabaseConnectionInfo(false);
    }
    _dataExtractor.getDataExtractorLog().info("Running on line update");
    _dataExtractorClient.run();
    _dataExtractor.getDataExtractorLog().closeLogFile();
    int returnCode = 0;
    if (_dataExtractor.getDataExtractorLog().hasErrorMessages()) {
      returnCode = 3;
      _dataExtractor.getDataExtractorLog().displayToStdOut("The following errors occurred during execution.  Please check the log file for more details.");
      for (String error : _dataExtractor.getDataExtractorLog().getErrorMessages()) {
        _dataExtractor.getDataExtractorLog().displayToStdOut(error);
      }
    }
    DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
    er.complete = true;
    er.returnCode = returnCode;
    return er;
  }

  private DataExtractor.ExecuteResults doTestConnection() throws IOException {
    if (_decCommandLine.hasOption(TEST_MODE_ONLY)) {
      NaiveTrustProvider.setAlwaysTrust(true);
    }

    int returnCode = 0;
    // Test the connection to the database
    if (_dataExtractor.checkDatabase()) {
      _dataExtractor.getDataExtractorLog().info("Successfully connected to local Claim Center Guidewire database");
    } else {
      _dataExtractor.getDataExtractorLog().error("Could not connect to local Claim Center Guidewire database");
      returnCode = 2;
    }

    // Test the connection to the server
    if (_decCommandLine.hasOption(LOG_QUERY_NAMES_ONLY)) {
      _dataExtractorClient.setPrintFullQueries(false);
    }
    else if (_decCommandLine.hasOption(LOG_FULL_QUERIES)) {
      _dataExtractorClient.setPrintFullQueries(true);
    }

    try {
      _dataExtractorClient.updateDBMSParameter();
      _dataExtractorClient.updateQueries(true);
      _dataExtractor.getDataExtractorLog().info("Successfully connected to external Guidewire server");
    } catch (Throwable t) {
      _dataExtractor.getDataExtractorLog().error("Could not connect to external Guidewire server");
      _dataExtractor.getDataExtractorLog().error(t.getMessage());
      _dataExtractor.getDataExtractorLog().error(MiscUtils.stackTrace(t));
      returnCode = 1;
    }

    // Run the connection test tools (ping, etc) that we get from the server.  We run these
    // in general, but the OMIT_TEST_TOOLS option will turn them off
    if (_decCommandLine.hasOption(OMIT_TEST_TOOLS) == false && _dataExtractorClient.unsuccessfulConnectAttempt() == false) {
      TestToolResults info = new TestToolResults();
      TestConnectionTools tool = new TestConnectionTools(info);

      // Get commands to run from server
      TestToolCommands commands = _dataExtractorClient.requestTestToolCommands(tool.isWindows());

      // Run info gathering tools
      tool.runTools(_dataExtractor, commands);

      // Send the info
      _dataExtractorClient.sendTestToolResults(info);
    }

    // Close the log and finish
    _dataExtractor.getDataExtractorLog().closeLogFile();
    DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
    er.complete = true;
    er.returnCode = returnCode;
    return er;
  }


  /**
   * Do an initial extract.  This involves running the queries, writing the results to .csv files, verifying the files
   * and finally compressing them into a .zip file.
   *
   * @return results packet
   * @throws IOException
   * @throws SQLException
   */
  private DataExtractor.ExecuteResults doInitialExtract(long startTime, long maxRunTime) throws IOException, SQLException {
    _dataExtractor.getDataExtractorLog().info("Executing queries");
    _dataExtractor.query();

    if (maxRunTime > 0 && (startTime + maxRunTime < System.currentTimeMillis())) {
      _dataExtractor.getDataExtractorLog().info("Max runtime exceeded.  _startTime [" + startTime + "] _maxRunTime [" + maxRunTime + "]");
      DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
      er.complete = true;
      er.returnCode = 0;
      return er;
    }

    if (_decCommandLine.hasOption(VERIFY)) {
      _dataExtractor.getDataExtractorLog().info("Verifying");
      _dataExtractor.verifyFiles();
    }

    _dataExtractor.getDataExtractorLog().info("Compressing results");
    _dataExtractor.compressResults();

    int returnCode = 0;
    if (_dataExtractor.getDataExtractorLog().hasErrorMessages()) {
      returnCode = 3;
      _dataExtractor.getDataExtractorLog().displayToStdOut("The following errors occurred during execution.  Please check the log file for more details.");
      for (String error : _dataExtractor.getDataExtractorLog().getErrorMessages()) {
        _dataExtractor.getDataExtractorLog().displayToStdOut("ERROR: " + error);
      }
    }
    _dataExtractor.getDataExtractorLog().closeLogFile();
    DataExtractor.ExecuteResults er = _dataExtractor.new ExecuteResults();
    er.complete = true;
    er.returnCode = returnCode;
    return er;
  }

  /**
   * Get a set of queries and insert them into the passed in DataExtractor
   *
   * @throws ParserConfigurationException
   * @throws SAXException
   * @throws IOException
   */
  private void loadQueries() throws ParserConfigurationException, SAXException, IOException {
    _dataExtractor.getDataExtractorLog().info("Get queries to be executed");
    if (_decCommandLine.hasOption(QUERY) && _decCommandLine.hasOption(USE_LOCAL_QUERIES)) {
      // Read the queries from the configuration files, but only if the user is running with both the
      // query and useLocalQueries options.
      _dataExtractor.readQueries();
    } else {
      // Contact the server to get queries
      if (_decCommandLine.hasOption(TEST_MODE_ONLY)) {
        NaiveTrustProvider.setAlwaysTrust(true);
      }

      try {
        _dataExtractorClient.prepareToRun();
        _dataExtractor.getDataExtractorLog().info("Successfully downloaded queries from server");
      } catch (Throwable t) {
        _dataExtractor.getDataExtractorLog().info("Could not connect to server");
      }
    }
  }

  /**
   * Get a set of queries and insert them into the passed in DataExtractor
   *
   * @throws ParserConfigurationException
   * @throws SAXException
   * @throws IOException
   */
  private List<CSVFileDefinition>  loadCustomerCSVFileDefinition() throws ParserConfigurationException, SAXException, IOException {
    _dataExtractor.getDataExtractorLog().info("Get csv file definition to be uploaded");
    // Either read the local file for file definitions (only valid with file validation option) or contact the server to get queries
    if (_decCommandLine.hasOption(TEST_MODE_ONLY)) {
      NaiveTrustProvider.setAlwaysTrust(true);
    }

    if (_decCommandLine.hasOption(CUSTOMER_CSV_LOCAL_FILE_DEFINITION)) {
      try {
        ArrayList<CSVFileDefinition> csvFileDefinitions = new ArrayList<>();
        csvFileDefinitions.add(_dataExtractorClient.getCSVFileDefinitionFromLocalFile(_decCommandLine.getOptionValue(CUSTOMER_CSV_LOCAL_FILE_DEFINITION), _dataExtractor));
        return csvFileDefinitions;
      } catch (IOException e) {
        _dataExtractor.getDataExtractorLog().info("Could not read file definitions from local file : " + _decCommandLine.getOptionValue(CUSTOMER_CSV_LOCAL_FILE_DEFINITION));
        return null;
      }
    }  else {
      try {
        _dataExtractorClient.setCustomerCSVFileTypeParameter(_decCommandLine.getOptionValue(CUSTOMER_CSV_TYPE));
        List<CSVFileDefinition> csvFileDefinition = _dataExtractorClient.prepareToRunCustomerCSVFile(false);
        if(csvFileDefinition.isEmpty()) {
          _dataExtractor.getDataExtractorLog().info("Did not receive file definition from server; contact Guidewire for assistance");
          return null;
        }
        _dataExtractor.getDataExtractorLog().info("Successfully downloaded csv file definitions from server");
        return csvFileDefinition;
      } catch (Throwable t) {
        _dataExtractor.getDataExtractorLog().info("Could not connect to server");
        return null;
      }
    }
  }



  private void verifyDirectoryOfFilesMatchesUserID(String filename) {
    String customerId = _dataExtractor.getCustomerID();
    if (customerId.indexOf('/') != -1) {
      // If we're using a username@guidewire.com/customer user.  So strip off the first part to get to just the customer
      // we're doing the upload for
      customerId = customerId.substring(customerId.indexOf('/') + 1);
    }
    if (filename.startsWith(customerId)) return;
    _dataExtractor.getDataExtractorLog().info("File name does not indicate the user ID to be used.");
    _dataExtractor.getDataExtractorLog().info("Files are prefixed with: " + filename);
    _dataExtractor.getDataExtractorLog().info("User ID: " + _dataExtractor.getUsername());
    System.out.println("Do you want to continue anyway?(yn)");
    // TODO: It would be nice to remember what the response is, rather than require it for every file

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String response;
    //  read the response from the command line; need to use try/catch with the
    //  readLine() method
    try {
      response = br.readLine();
      if (response.contains("y") || response.contains("Y")) return;
      _dataExtractor.getDataExtractorLog().info("Exiting based on response: " + response);
      System.exit(0);
    } catch (IOException ioe) {
      System.out.println("IO error trying to read your name!");
      System.exit(1);
    }
  }

  private boolean sendInitialCsvFile(File csvFile, String name) throws FileNotFoundException {
    String fileName = csvFile.getName();
    int underscoreIndex = fileName.indexOf('_');
    if (underscoreIndex == -1) {
      _dataExtractor.getDataExtractorLog().info("File [" + fileName + "] should have a customer name as a prefix.");
      _dataExtractor.getDataExtractorLog().closeLogFile();
      return false;
    }
    String clientShortName = fileName.substring(0, underscoreIndex);
    verifyDirectoryOfFilesMatchesUserID(clientShortName);

    _dataExtractor.getDataExtractorLog().info("Sending data to [" + name + "] from csv file [" + csvFile + "]");
    if (csvFile.exists() == false) {
      _dataExtractor.getDataExtractorLog().info("File [" + fileName + "] cannot be found for csv import");
      _dataExtractor.getDataExtractorLog().closeLogFile();
      return false;
    }
    InputStream stream = new FileInputStream(csvFile);
    _dataExtractorClient.runFromFile(name, stream);
    return true;
  }

  /**
   * Send a customer csv file to the DES.  Performs checks on the file, like whether it exists, is a .csv file in our
   * expected format, etc.
   *
   * @param customerCsvFile File to send
   * @return true iff the data is successfully sent to the DES
   */
  private boolean sendCustomerCsvFile(File customerCsvFile, CSVFileDefinition csvFileDefinition) throws IOException {
    CustomerCSVFileDataSource customerCSVFileDataSource = new CustomerCSVFileDataSource(customerCsvFile, _dataExtractor, csvFileDefinition);
    if (!customerCSVFileDataSource.successfullyReadHeaders()) {
      // In this case, we assume that the constructor for CustomerCSVFileDataSource has written a useful error message
      return false;
    }

    _dataExtractor.getDataExtractorLog().info("Sending customer csv file [" + customerCsvFile.getName() + "]");
    _dataExtractorClient.sendCustomerCsvFile(customerCSVFileDataSource);
    return true;
  }

  /**
   * Send a customer csv file to the DES.  Performs checks on the file, like whether it exists, is a .csv file in our
   * expected format, etc.
   *
   * @param customerCsvFile File to send
   * @param csvFileDefinition The file definition that we retrieve from the DES. Data in the csv file must conform to the requirements of the file definition
   * @return true iff the data is successfully sent to the DES
   */
  private boolean validateCustomerCSVFile(File customerCsvFile, CSVFileDefinition csvFileDefinition) throws IOException {
    CustomerCSVFileDataSource customerCSVFileDataSource = new CustomerCSVFileDataSource(customerCsvFile, _dataExtractor, csvFileDefinition, true);
    if (!customerCSVFileDataSource.successfullyReadHeaders()) {
      // In this case, we assume that the constructor for CustomerCSVFileDataSource has written a useful error message
      return false;
    }

      _dataExtractor.getDataExtractorLog().info("Validating customer csv file [" + customerCsvFile.getName() + "]");
      try {
        _dataExtractorClient.validateCustomerCsvFile(customerCSVFileDataSource);
      } catch (Exception e) {
        _dataExtractor.getDataExtractorLog().error("Errors found while validating csvfile");
      }
      return true;
  }


  private DataExtractor.ExecuteResults handleCustomerCSVRequests() throws ParserConfigurationException, SAXException, IOException, SQLException {
    if (_decCommandLine.hasOption(CUSTOMER_CSV) || _decCommandLine.hasOption(CUSTOMER_CSV_VALIDATE)) {
      if (!_decCommandLine.hasOption(CUSTOMER_CSV_TYPE)) {
        _dataExtractor.getDataExtractorLog().error("The data type of the .csv is required");
        return createExecuteResults(1);
      }

      List<CSVFileDefinition> csvFileDefinition = loadCustomerCSVFileDefinition();
      if (csvFileDefinition == null) {
        return createExecuteResults(1);
      }
      return handleCustomerCsvData(csvFileDefinition.get(0));
    }  else if (_decCommandLine.hasOption(CUSTOMER_CSV_FILE_DEF_UPLOAD)) {
      return doCustomerCSVFileDefUpload();
    }
    return createExecuteResults(1);
  }

  private boolean hasCustomerCSVOption() {
    return _decCommandLine.hasOption(CUSTOMER_CSV_FILE_DEF_UPLOAD) ||
      _decCommandLine.hasOption(CUSTOMER_CSV_VALIDATE) ||
      _decCommandLine.hasOption(CUSTOMER_CSV);
  }


  private DataExtractor.ExecuteResults handleCustomerCsvData(CSVFileDefinition fileDefinition) throws SQLException, IOException {
    String csvFileName = _decCommandLine.hasOption(CUSTOMER_CSV) ?
      _decCommandLine.getOptionValue(CUSTOMER_CSV) : _decCommandLine.getOptionValue(CUSTOMER_CSV_VALIDATE);
    if (csvFileName == null) {
      _dataExtractor.getDataExtractorLog().error("The data file to be uploaded is required");
      return createExecuteResults(1);
    }
    _dataExtractor.getDataExtractorLog().info("Executing customer csv upload from file [" + csvFileName + "]");
    int returnCode = 0;

    File customerCsvFile = new File(csvFileName);
    if (_decCommandLine.hasOption(CUSTOMER_CSV_VALIDATE)) {
      if (!validateCustomerCSVFile(new File(csvFileName), fileDefinition)) {
        returnCode = 1;
      }
    } else{
      if (!sendCustomerCsvFile(customerCsvFile, fileDefinition)) {
        returnCode = 1;
      }
      _dataExtractorClient.sendSummary(DataExtractorClient.UploadType.CUSTOMER_CSV);
    }
    return createExecuteResults(returnCode);

  }

  private DataExtractor.ExecuteResults doCustomerCSVFileDefUpload() throws SQLException, IOException {
    if (_decCommandLine.hasOption(TEST_MODE_ONLY)) {
      NaiveTrustProvider.setAlwaysTrust(true);
    }
    String csvFileDefName = _decCommandLine.getOptionValue(CUSTOMER_CSV_FILE_DEF_UPLOAD);
    if (csvFileDefName == null) {
      _dataExtractor.getDataExtractorLog().error("The file definition file to be uploaded is required");
      return createExecuteResults(1);
    }
    _dataExtractor.getDataExtractorLog().info("Executing customer csv file definition upload from file [" + csvFileDefName + "]");
    int returnCode = 0;

    CSVFileDefinition csvFileDefinition = _dataExtractorClient.getCSVFileDefinitionFromLocalFile(csvFileDefName, _dataExtractor);
    if (!_dataExtractorClient.sendCustomerCsvFileDefinition(csvFileDefinition)) {
      returnCode = 1;
    }
    return createExecuteResults(returnCode);
  }



  /////////////////////////////////////////////////////////////////////

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    int exitCode;
    try {
      DataExtractionRunner dataExtractionRunner = new DataExtractionRunner(args);
      exitCode = dataExtractionRunner.execute();
    } catch (Exception e) {
      throw new RuntimeException("Could not complete run", e);
    }

    System.exit(exitCode);
  }


}
