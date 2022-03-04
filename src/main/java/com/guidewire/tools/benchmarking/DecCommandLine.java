package com.guidewire.tools.benchmarking;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;


/**
 * DataExtractionRunner takes in the users commands, creates the necessary objects and performs the actions for the user.
 * To execute the commands, the DER will catch exceptions and reattempt in order to get around temporary
 * conditions (DB or web maintenance, server resets, etc.)
 */
public class DecCommandLine {

  private static final int INITIAL_RETRY_INTERVAL = 5 * 60;
  private static final int NUMBER_OF_RETRIES = 2;
  private static final int RETRY_INTERVAL_MULTIPLIER = 2;

  // command line options
  static final String CLIENT = "client";
  static final String CSV_FILE = "csvFile";
  static final String CUSTOMER_CSV = "customer_csv";
  static final String CUSTOMER_CSV_FILE_DEF_UPLOAD = "upload_customer_csv_filedef";
  static final String CUSTOMER_CSV_LOCAL_FILE_DEFINITION = "customer_csv_validate_local_file_def";
  static final String CUSTOMER_CSV_TYPE = "customer_csv_type";
  static final String CUSTOMER_CSV_VALIDATE = "customer_csv_validate";
  static final String ENCRYPT = "encrypt";
  static final String FILE = "file";
  static final String HELP = "help";
  static final String LOG_FULL_QUERIES = "logfullqueries";
  static final String LOG_QUERY_NAMES_ONLY = "logquerynamesonly";
  static final String NAME = "name";
  static final String NO_DATABASE_CONNECTION_INFO = "noDatabaseConnectionInfo";
  static final String OMIT_TEST_TOOLS = "omittesttools";
  public static final String ON_LINE_UPDATE = "onlineupdate";
  static final String QUERY = "query";
  static final String SHOW_HIDDEN_OPTIONS = "showhiddenoptions";
  static final String SINCE = "since";
  static final String TEST_CONNECTION = "testconnection";
  static final String TEST_MODE_ONLY = "testmodeonly";
  static final String TOP = "top";
  static final String USE_LOCAL_QUERIES = "uselocalqueries";
  static final String VERIFY = "verify";
  static final String VERSION = "version";

  private static final String COMPRESS = "compress";
  private static final String MAX_RUN_TIME = "maxRunTime";
  public static final String RETRIES_OPTION = "retries";
  public static final String RETRY_INTERVAL_OPTION = "retryinterval";
  public static final String RETRY_MULTIPLIER_OPTION = "retryintervalmultiplier";


  private static final int DEFAULT_MAX_RUNTIME = -1;


  private final CommandLine _commandLine;

  private final long _startTime;
  private final int _numberOfRetries;
  private final long _maxRunTimeMilli;
  private final int _retryIntervalMilli;
  private final int _retryMultiplier;


  private int getIntOption(String option, int defaultValue) {
    int value = defaultValue;
    if (_commandLine.hasOption(option)) {
      String sValue = _commandLine.getOptionValue(option);
      try {
        int iValue = Integer.parseInt(sValue);
        if (iValue > 0) {
          value = iValue;
        }
      } catch (NumberFormatException nfe) {
        System.err.println("Cannot read value for " + option + ": " + _commandLine.getOptionValue(option));
        nfe.printStackTrace();
      }
    }

    return value;
  }


  /**
   * Constructor
   */
  DecCommandLine(String[] args) throws Exception{
    _commandLine = getCommandLine(args);
    _startTime = System.currentTimeMillis();

    _numberOfRetries = getIntOption(RETRIES_OPTION, NUMBER_OF_RETRIES);
    _retryMultiplier = getIntOption(RETRY_MULTIPLIER_OPTION, RETRY_INTERVAL_MULTIPLIER);
    _retryIntervalMilli = 1000 * getIntOption(RETRY_INTERVAL_OPTION, INITIAL_RETRY_INTERVAL);

    // If _maxRunTimeMilli is -1 (the default), no maximum will be used.  Otherwise, _maxRunTimeMilli will
    // contain the maximum in milliseconds that the client should run.  Note that this means we
    // must convert the option value, since it will be in minutes.  Note also that the _maxRunTimeMilli
    // option is only available for onLineUpdate.
    int maxRunTimeInMinutes = getIntOption(MAX_RUN_TIME, DEFAULT_MAX_RUNTIME);
    _maxRunTimeMilli = _commandLine.hasOption(ON_LINE_UPDATE) && maxRunTimeInMinutes != DEFAULT_MAX_RUNTIME ?
      maxRunTimeInMinutes * 60 * 1000 :
      DEFAULT_MAX_RUNTIME;
  }


  int getNumberOfRetries() {
    return _numberOfRetries;
  }

  long getStartTime() {
    return _startTime;
  }

  long getMaxRunTimeMilli() {
    return _maxRunTimeMilli;
  }

  int getRetryIntervalMilli() {
    return _retryIntervalMilli;
  }

  int getRetryMultiplier() {
    return _retryMultiplier;
  }


  boolean hasOption(String option) {
    return _commandLine.hasOption(option);
  }


  String getOptionValue(String option) {
    return _commandLine.getOptionValue(option);
  }


  Options getAllCommandLineOptions() {
    Options result = getCommandLineOptions();
    for (Object hiddenOption : getHiddenOptions().getOptions()){
      result.addOption((Option)hiddenOption);
    }
    return result;
  }


  Options getCommandLineOptions() {
    Options options = new Options();
    options.addOption("h", HELP, false, "display this message");
    options.addOption("f", FILE, true, "specify the config file to use.  Defaults to config.xml");
    options.addOption("v", VERSION, false, "returns the version number of this application");
    options.addOption("t", TOP, false, "run the query, but only write the first 25 rows of results");
    options.addOption("q", QUERY, false, "run the query(s) to create the output file(s)");
    options.addOption("u", ON_LINE_UPDATE, false, "run the query(s) to create the output file(s)");
    options.addOption("r", RETRIES_OPTION, true, "The number of times to retry if the operation fails");
    options.addOption("m", RETRY_MULTIPLIER_OPTION, true, "Multiplier for the interval between retries.  For example, if the multiplier is 2, each retry will wait twice as long as the previous try before trying again");
    options.addOption("w", RETRY_INTERVAL_OPTION, true, "Time in seconds to wait between retries.");
    options.addOption(TEST_CONNECTION, false, "test the connection to the Guidewire server");
    options.addOption(MAX_RUN_TIME, true, "Maximum time (in minutes) that the client should run.  This option is only in effect for on line update.");
    options.addOption("d", NO_DATABASE_CONNECTION_INFO, false, "Don't send database connection info to Guidewire");
    options.addOption("e", ENCRYPT, true, "Encrypt the argument");
    return options;
  }

  private Options getHiddenOptions() {
    Options options = new Options();
    options.addOption(SHOW_HIDDEN_OPTIONS, false, "Displays all options, including the hidden ones");
    options.addOption(TEST_MODE_ONLY, false, "Do not require certification to run");
    options.addOption(VERIFY, false, "verify the output files have as many output rows as expected");
    options.addOption(COMPRESS, false, "compress the output files into a zip file");
    options.addOption("s", SINCE, true, "queries for values created since date provided");
    options.addOption("csv", CSV_FILE, true, "CSV file to upload");
    options.addOption("n", NAME, true, "name of table to upload CSV file to");
    options.addOption(USE_LOCAL_QUERIES, false, "Use the queries from the local configuration rather than contacting the Guidewire server for queries.  This option is only valid with the -query option.");
    options.addOption(OMIT_TEST_TOOLS, false, "Do not run the test tools that are usually run as part of a test connection");
    options.addOption(LOG_QUERY_NAMES_ONLY, false, "Causes a test connection to only write the names of queries to the log, rather than the entire query");
    options.addOption(LOG_FULL_QUERIES, false, "Causes a test connection to write the full queries to the log");
    options.addOption(CLIENT, true, "The client this action is for.");

    options.addOption("c", CUSTOMER_CSV, true, "Upload a customer .csv file");
    options.addOption("ctype", CUSTOMER_CSV_TYPE, true, "The type of the customer .csv file");
    options.addOption("cv", CUSTOMER_CSV_VALIDATE, true, "Validate a customer .csv file");
    options.addOption("cvfd", CUSTOMER_CSV_LOCAL_FILE_DEFINITION, true, "A local file with the file definitions to validate a customer .csv file. Only valid in Validate mode");
    options.addOption("ccfdu", CUSTOMER_CSV_FILE_DEF_UPLOAD, true, "CSV file definition to read and upload");

    return options;
  }


  String getCommandLineString() {
    // Assemble the command line options into a string and write them out.  This is done after
    // the data extractor is built because we will write using the extractor logging facilities.
    Option[] opts = _commandLine.getOptions();
    StringBuilder sb = new StringBuilder();
    sb.append("dataExtractionRunner");
    for (Option opt : opts) {
      sb.append(" -");
      String name = opt.getLongOpt();
      if (name == null || name.length() <= 0) {
        name = opt.getOpt();
      }
      sb.append(name);

      String val = opt.getValue();
      if (val != null && val.length() > 0) {
        sb.append(" ");
        sb.append(val);
      }
    }
    return sb.toString();
  }



  private static List<String> validOptionStrings;
  {
    validOptionStrings = new ArrayList<>();
    validOptionStrings.add(ON_LINE_UPDATE);
    validOptionStrings.add(COMPRESS);
    validOptionStrings.add(VERIFY);
    validOptionStrings.add(CSV_FILE);
    validOptionStrings.add(TEST_CONNECTION);
    validOptionStrings.add(CUSTOMER_CSV);
    validOptionStrings.add(CUSTOMER_CSV_VALIDATE);
    validOptionStrings.add(CUSTOMER_CSV_FILE_DEF_UPLOAD);
    validOptionStrings.add(QUERY);
    validOptionStrings.add(ENCRYPT);
  }

  boolean hasNoValidOptions() {
    for (String validOption : validOptionStrings) {
      if (_commandLine.hasOption(validOption)) {
        return false;
      }
    }
    return true;
  }



  /**
   * Parse the command line args
   */
  private CommandLine getCommandLine(String[] args) throws Exception {
    int i = 0;
    while (i < args.length) {
      // We do not support upper and lower case options with the same character - that is "-x" is
      // the same as "-X".  This is because CommandLine will give a somewhat unhelpful message, and
      // we don't want to make life harder for our customers.  But note that we only change the option
      // designator itself, never any values.  Otherwise we screw up strings for the encrypt option,
      // among other potential problems.
      if (args[i].charAt(0) == '-') {
        args[i] = args[i].toLowerCase();
      }
      i++;
    }

    Options allOptions = getAllCommandLineOptions();
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(allOptions, args);

    return cmd;
  }



}
