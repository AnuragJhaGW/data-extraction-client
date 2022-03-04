package com.guidewire.tools.benchmarking;

import au.com.bytecode.opencsv.CSVWriter;
import com.guidewire.tools.DataExtractionUtils;
import com.guidewire.tools.MiscUtils;
import com.guidewire.tools.PropertiesUtils;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import oracle.jdbc.driver.OracleDriver;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.lang.time.DateUtils;
import org.xml.sax.SAXException;

import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



/**
 * DataExtractor runs queries against a database and provides the results for uploading or writes them to file.  In
 * order to do it's job, it is able to do several things.  Obviously, it must be able to run queries against a database,
 * holding on to the configuration necessary to connect and get results.  As part of running the queries, it has the
 * ability to load, modify and update those queries.  The queries are written with a Guidewire specific notation
 * that allows a QueryDefinition to be fully configured.  Using properties files, DataExtractor will substitute various
 * elements in queries.  This can account for columns, joins, where clauses and even entire queries specific to a
 * customer.  It can also account for differences between application versions or DBMS syntax.
 *
 * Included in the DataExtractor is the ability to write a simple logWriter file on behalf of the client application.
 *
 */
@SuppressWarnings("WeakerAccess")
public class DataExtractor {

  private static final String DB_PROPERTY_FILE = "db";
  private static final String APP_VERSION_PROPERTY_FILE = "appVersion";
  public static final String DEFAULT_CUSTOMER_PROPERTIES = "default_customer.properties";

  private static final int BUFFER = 2048;
  private static final int ROWS_TO_WRITE_BEFORE_NOTIFICATION = 10000;
  public static final String AURORA = "aurora";
  public static final String ORACLE = "oracle";
  public static final String SQLSERVER = "sqlserver";
  private static final String ORACLE_TEST_QUERY = "select 1 from dual";
  protected static final String CLIENT_VERSION_PROPERTY = "guidewire.extractor.version";

  // The next set of static final objects contain the values for various fields as they are set in
  // the standard config template.  We use them to check that the user has changed them in some way.
  // If they haven't been changed, it likely means that the customer has not correctly edited the
  // config file.
  private static final String UNCONFIGED_DBURL = "DB_URL";
  private static final String UNCONFIGED_DB = "DB_NAME";
  private static final String UNCONFIGED_DB_USER = "USERID";
  private static final String UNCONFIGED_DB_PASSWORD = "DB";
  private static final String UNCONFIGED_GUIDEWIRE_USER = "GUIDEWIRE_UPLOAD_USER";
  private static final String UNCONFIGED_GUIDEWIRE_PASSWORD = "GUIDEWIRE_PASSWORD";

  private Connection dbConnection = null;
  private String dbURL;
  private String dbUserID;
  private String dbUserPassword;
  private String db = "";
  private List<QueryDefinition> queries = new ArrayList<>();
  private List<CSVFileDefinition> customerFileDefinitions = new ArrayList<>();
  private String outputDir = "working";

  private String username;
  private File queryConfigFile;
  private String configFilePath;
  private String client; // the client we're working for.  May be an id, display name or cbsclientname.

  private String queryConfigFileName;
  private int ccVersion;
  private String guidewireClientVersion;
  private boolean top = false;
  private static final int UNKNOWN_ROW_COUNT = -1;

  private DataExtractorLog _dataExtractorLog = new DataExtractorLog();
  private String _phrase;

  private String dbTag = "";
  private String dbType = "";
  private Date dataFrom = null;
  private String uploadURL = "https://cbs.guidewire.com/dataExtractionServer";
  private String domain = "cbs.guidewire.com";
  private String casHost = "gw-loginservice-prod.guidewire.net";
  private String guidewirePassword;
  private String gwAuth;
  private String oktaHost;
  private String oktaClientId;
  private String oktaClientSecret;
  private String oktaClientToken;


  // The following set of variables is used only when the customer needs to connect through a proxy
  private String proxyHost = null;
  private String proxyPort = null;
  private String proxyUsername = null;
  private String proxyPassword = null;
  private String proxyDomain = null;
  private int proxyAuthenticationSchema = NO_PROXY_AUTH;
  public static final int NO_PROXY_AUTH = -1;
  public static final int PROXY_AUTH_BASIC = 1;
  public static final int PROXY_AUTH_NTLM = 2;


  // The tableNameMap maps queries to the names of the tables that they query to get their createTime
  // fields.  This allows us to construct a query to get the earliest value for the createTime field,
  // so we can know when we should be done querying in the case of chunked tables.
  private static final Map<String, String> tableNameMap = new HashMap<String, String>(){
    {
      put("transaction", "cc_transaction");
      put("transactionlineitem", "cc_transactionlineitem");
      put("check", "cc_check");
      put("recovery_payer", "cc_transaction");  // Note that recovery_payer gets it's createTime from cc_transaction
      put("claim_policy", "cc_policy");
      put("claim", "cc_claim");
      put("exposure", "cc_exposure");
      put("peril", "none");  // This query does not have a create time, and so does not chunk
      put("zone", "none"); // This query does not have a create time, and so does not chunk
      put("catDescriptionL10N", "none"); // This query does not have a create time, and so does not chunk
      put("catNameL10N", "none"); // This query does not have a create time, and so does not chunk
      put("claimindicator", "cc_claimindicator");
      put("incident", "cc_incident");
      put("bodypart", "cc_bodypart");
      put("coverage", "cc_coverage");
    }
  };

  private DateFormat queryDateFormatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

  public String getOktaHost() {
    return oktaHost;
  }

  public void setOktaHost(String oktaHost) {
    this.oktaHost = oktaHost;
  }

  public String getOktaClientId() {
    return oktaClientId;
  }

  public void setOktaClientId(String oktaClientId) {
    this.oktaClientId = oktaClientId;
  }

  public String getOktaClientSecret() {
    return oktaClientSecret;
  }

  public void setOktaClientSecret(String oktaClientSecret) {
    this.oktaClientSecret = oktaClientSecret;
  }

  public String getOktaClientToken() {
    return oktaClientToken;
  }

  public void setOktaClientToken(String oktaClientToken) {
    this.oktaClientToken = oktaClientToken;
  }

  protected class ExecuteResults {
    boolean complete = false;
    int returnCode = 0;
  }


  /**
   * This constructor is only used by tests.
   */
  public DataExtractor() {
    _dataExtractorLog.createLogFile();
  }


  //FileConfigProcessor


  public DataExtractor(String configurationFile, String clientVersion, String phrase) throws Exception {

    this(new FileConfigProcessor(configurationFile), clientVersion, phrase);
  }


  protected DataExtractor(ConfigProcessor configProcessor, String clientVersion, String phrase) throws Exception {
    _phrase = phrase;
    guidewireClientVersion = clientVersion;
    _dataExtractorLog.createLogFile();
    configProcessor.process(this);
  }


  public void finalize() {
    _dataExtractorLog.closeLogFile();
  }


  protected void setGuidewireClientVersion(String clientVersion) {
    guidewireClientVersion = clientVersion;
  }

  public String getGuidewireClientVersion() {
    return guidewireClientVersion;
  }


  public int getVersion() {
    return ccVersion;
  }

  public void setCCVersion(String sCcVersion) {
    this.ccVersion = MiscUtils.ccVersion(sCcVersion);
  }

  private boolean matchesVersion(String version, String incoming) {
    if (incoming == null) return false;
    Pattern pattern = Pattern.compile(version);
    Matcher matcher = pattern.matcher(incoming);
    return matcher.find();
  }

  public DataExtractorLog getDataExtractorLog() {
    return _dataExtractorLog;
  }

  protected void setSince(String yyyyMMdd) throws ParseException {
    _dataExtractorLog.info("Will extract data since [" + yyyyMMdd + "]");
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    setSince(format.parse(yyyyMMdd));
  }

  private void setSince(Date date) {
    dataFrom = date;
  }

  protected List<QueryDefinition> getAllQueries() {
    return queries;
  }

  protected List<CSVFileDefinition> getAllCSVFileDefinitions() {
    return customerFileDefinitions;
  }

  public String getDBTag() {
    return dbTag;
  }

  public void setDBTag(String DBTag) {
    if (DBTag != null && DBTag.length() > 0 && DBTag.charAt(DBTag.length()-1) != '.') {
      // If DBTag has a value, but there is no . at the end of it, we must add one, since
      // the queries are all of the form &DBTAG<tablename>.  So there must be a . in the
      // substitution string, and if the customer hasn't added it, we will put it in for
      // them.
      DBTag += '.';
    }
    dbTag = DBTag;
  }

  public String getDBType() {
    if (dbType == null || dbType.length() == 0) {
      return getDerivedDBType();
    }
    return dbType;
  }

  private String getDerivedDBType() {
    String dbUrl = getDBURL();
    if (dbUrl == null) {
      _dataExtractorLog.error("Your config file is missing the 'url' attribute in the <database> element.");
      return null;
    }

    if (dbUrl.toLowerCase().contains(ORACLE)) {
      return ORACLE;
    }
    if (dbUrl.toLowerCase().contains(SQLSERVER)) {
      return SQLSERVER;
    }
    if (dbUrl.toLowerCase().contains("postgresql")) {
      return AURORA;
    }
    if (successfullyRunsOracleQuery()) {
      return ORACLE;
    }
    return SQLSERVER;
  }

  public void setDBType(String DBType) {
    dbType = DBType;
  }

  protected void addQuery(QueryDefinition currentQueryDefinition) {
    queries.add(currentQueryDefinition);
  }

  protected void addCSVFileDefinition(CSVFileDefinition currentCSVFileDefinition) {
    customerFileDefinitions.add(currentCSVFileDefinition);
  }

  public String getDBURL() {
    return dbURL;
  }

  public void setDBURL(String dbURL) {
    if (dbURL != null && dbURL.equals(UNCONFIGED_DBURL)) {
      _dataExtractorLog.error("The database url field in the config file still contains the default value.  Please check your config file to make sure it has been correctly configured.");
    }
    this.dbURL = dbURL;
  }

  public String getDBUserID() {
    return dbUserID;
  }

  public void setDBUserID(String dbUserID) {
    if (dbUserID != null && dbUserID.equals(UNCONFIGED_DB_USER)) {
      _dataExtractorLog.error("The database user field in the config file still contains the default value.  Please check your config file to make sure it has been correctly configured.");
    }
    this.dbUserID = dbUserID;
  }


  public void setDBUserPassword(String dbUserPassword) {
    if (dbUserPassword != null && dbUserPassword.equals(UNCONFIGED_DB_PASSWORD)) {
      _dataExtractorLog.error("The database password field in the config file still contains the default value.  Please check your config file to make sure it has been correctly configured.");
    }
    this.dbUserPassword = dbUserPassword;
  }


  /**
   * for testing
   */
  public String getDbUserPassword() {
    return dbUserPassword;
  }


  public void decryptDBUserPassword() {
    if (dbUserPassword != null) {
      byte[] passwordBytes = Base64.getDecoder().decode(dbUserPassword);

      // Note that we set up the cipher before we parse the config file - this is necessary because if there is encryption, the
      // cipher will need to be available when the parser calls for it.
      //
      try {
        ByteBuffer byteBuffer = ByteBuffer.wrap(passwordBytes);
        int nonceSize = byteBuffer.getInt();
        if(nonceSize < 12 || nonceSize >= 16) {
          throw new IllegalArgumentException("Nonce size is incorrect. Make sure that the incoming data is an AES encrypted file.");
        }
        byte[] iv = new byte[nonceSize];
        byteBuffer.get(iv);
        SecretKey secretKey = DataExtractionRunner.generateSecretKey(_phrase, iv);
        byte[] cipherBytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(cipherBytes);
        Cipher aes = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv);
        aes.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);
        byte[] decryptedResult = aes.doFinal(cipherBytes);
        dbUserPassword = new String(decryptedResult, StandardCharsets.UTF_8);
      } catch (NoSuchAlgorithmException e) {
        _dataExtractorLog.info("Parser Configuration error. Could not configure encryption, got NoSuchAlgorithmException [" + e.getLocalizedMessage() + "]");
        e.printStackTrace();
      } catch (NoSuchPaddingException e) {
        _dataExtractorLog.info("Parser Configuration error. Could not configure encryption, got NoSuchPaddingException [" + e.getLocalizedMessage() + "]");
        e.printStackTrace();
      } catch (InvalidKeyException e) {
        _dataExtractorLog.info("Parser Configuration error. Could not configure encryption got InvalidKeyException [" + e.getLocalizedMessage() + "]");
        e.printStackTrace();
      } catch (IllegalBlockSizeException e) {
        _dataExtractorLog.info("Error decrypting password, got IllegalBlockSizeException [" + e.getLocalizedMessage() + "]");
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      } catch (BadPaddingException e) {
        _dataExtractorLog.info("Error decrypting password, got BadPaddingException [" + e.getLocalizedMessage() + "]");
        e.printStackTrace();
      } catch (InvalidAlgorithmParameterException e) {
        _dataExtractorLog.info("Error decrypting password, got InvalidAlgorithmParameterException [" + e.getLocalizedMessage() + "]");
        e.printStackTrace();
      } catch (InvalidKeySpecException e) {
        _dataExtractorLog.info("Error decrypting password, got InvalidKeySpecException [" + e.getLocalizedMessage() + "]");
        e.printStackTrace();
      }
    }
  }

  public String getUploadURL() {
    return uploadURL;
  }

  public void setUploadURL(String url) {
    uploadURL = url;
  }

  public String getCasHost() {
    return casHost;
  }

  public void setCasHost(String host) {
    casHost = host;
  }

  public String getDomain() {
    return domain;
  }

  public void setDomain(String d) {
    domain = d;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public void setProxyHost(String proxy) {
    proxyHost = proxy;
  }

  public String getProxyPort() {
    return proxyPort;
  }

  public void setProxyPort(String port) {
    try {
      Integer.parseInt(port); // make sure it's an int
      proxyPort = port;
    } catch (NumberFormatException nfe) {
      _dataExtractorLog.errorToLogMsgToConsole(nfe, "Error with proxy port [" + port + "] - value must be an integer.  Using default value of 8080");
      proxyPort = "8080";
    }
  }

  public String getProxyUsername() {
    return proxyUsername;
  }

  public void setProxyUsername(String username) {
    proxyUsername = username;
  }

  public String getProxyPassword() {
    return proxyPassword;
  }

  public void setProxyPassword(String password) {
    proxyPassword = password;
  }

  public String getProxyDomain() {
    return proxyDomain;
  }

  public void setProxyDomain(String domain) {
    proxyDomain = domain;
  }

  public int getProxyAuthenticationSchema() {
    return proxyAuthenticationSchema;
  }

  public void setProxyAuthenticationSchema(String scheme) {
    if ("basic".equalsIgnoreCase(scheme))
      proxyAuthenticationSchema = PROXY_AUTH_BASIC;
    else if ("ntlm".equalsIgnoreCase(scheme))
      proxyAuthenticationSchema = PROXY_AUTH_NTLM;
    else if ("none".equalsIgnoreCase(scheme))
      proxyAuthenticationSchema = NO_PROXY_AUTH;
    else {
      proxyAuthenticationSchema = NO_PROXY_AUTH;
      _dataExtractorLog.error("Invalid proxy authentication scheme [" + scheme + "], using no proxy authentication.");
    }
  }


  public String getGuidewirePassword() {
    return guidewirePassword;
  }


  public void setGuidewirePassword(String password) {
    if (password != null && password.equals(UNCONFIGED_GUIDEWIRE_PASSWORD)) {
      _dataExtractorLog.error("The guidewire password field in the config file still contains the default value.  Please check your config file to make sure it has been correctly configured.");
    }
    guidewirePassword = password;
  }


  public String getGwAuth() {
    return gwAuth;
  }

  public void setGwAuth(String gwAuth) {
    this.gwAuth = gwAuth;
  }

  protected boolean isReadyToQuery() {
    return isDBFullyConfigured() || checkDatabase();
  }

  private boolean isDBFullyConfigured() {
    return dbURL != null &&
            (dbURL.contains(ORACLE) && isReadyToQueryOracle()) ||
            isReadyToQuerySQLServer();
  }

  private boolean isReadyToQueryOracle() {
    return (!dbURL.contains("USERID") || dbUserID != null) &&
            (!dbURL.contains("PASSWORD") || dbUserPassword != null);
  }

  private boolean isReadyToQuerySQLServer() {
    return db != null &&
            dbUserID != null &&
            dbUserPassword != null;
  }


  public String getUsername() {
    return username;
  }

  public void setUsername(String name) {
    if (name != null && name.equals(UNCONFIGED_GUIDEWIRE_USER)) {
      _dataExtractorLog.error("The guidewire username field in the config file still contains the default value.  Please check your config file to make sure it has been correctly configured.");
    }
    username = name;
  }


  public void loadQueryConfiguration(InputStream baseQueryConfig, InputStream additionalQueries, InputStream custFile, InputStream dbFile, InputStream versionFile, InputStream fieldMapStream) {
    if (dbFile == null) {
      throw new RuntimeException("Need to specify DB properties file with -" + DB_PROPERTY_FILE);
    }
    if (versionFile == null) {
      throw new RuntimeException("Need to specify ccVersion properties file with -" + APP_VERSION_PROPERTY_FILE);
    }

    try {
      queries = new ArrayList<QueryDefinition>();
      if (baseQueryConfig != null) {
        readQueries(baseQueryConfig);
      }
      if (additionalQueries != null) {
        readQueries(additionalQueries);
      }
      transformQueries(queries, custFile, dbFile, versionFile, fieldMapStream);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  public void transformQueries(List<QueryDefinition> queries, InputStream custFile, InputStream dbFile, InputStream versionFile, InputStream fieldMapStream) throws IOException {
    Properties customerProperties = getDefaultCustomerProperties();
    customerProperties = PropertiesUtils.getProperties(customerProperties, custFile);
    Properties versionProperties = PropertiesUtils.getProperties(versionFile);
    Properties dbProperties = PropertiesUtils.getProperties(dbFile);
    Properties fieldMapProperties = null;
    if (fieldMapStream != null)
      fieldMapProperties = PropertiesUtils.getProperties(fieldMapStream);
    transformQueries(queries, customerProperties, versionProperties, dbProperties, fieldMapProperties);
  }

  private Properties getDefaultCustomerProperties() throws IOException {
    return PropertiesUtils.getProperties(DataExtractionUtils.getInputStreamBasedOnFileName(DEFAULT_CUSTOMER_PROPERTIES));
  }

  public void transformQueries(List<QueryDefinition> queries, Properties customerProperties, Properties versionProperties, Properties dbProperties, Properties fieldMapProperties) {
    for (QueryDefinition queryDefinition : queries) {
      if (customerProperties != null) {
        queryDefinition.transform(customerProperties);
      }

      if (fieldMapProperties != null) {
        queryDefinition.transform(fieldMapProperties);
      }

      queryDefinition.transform(versionProperties);
      queryDefinition.buildColumnDefs();
      queryDefinition.transform(dbProperties);
    }
  }



  private InputStream getResourceStream(String filename) {
    InputStream resourceStream = getClass().getClassLoader().getResourceAsStream(filename);
    if (resourceStream == null) {
      resourceStream = getClass().getClassLoader().getResourceAsStream(filename.toLowerCase());
    }
    return resourceStream;
  }

  /**
   * customer id is derived based on the upload username.  Most usernames are customerName + "upload".
   */
  protected String getCustomerID() {
    if (username == null) return "";
    return username.replace("upload", "");
  }

  public void setQueryConfigFileName(String fileName) {
    queryConfigFileName = fileName;
  }


  public void setQueryConfigFile(File file) {
    queryConfigFile = file;
  }

  public void readQueries() throws IOException, SAXException, ParserConfigurationException {
    InputStream inputStream = getInputStreamForQueries();
    if (inputStream == null) {
      _dataExtractorLog.error("No input stream available for reading");
      return;
    }
    readQueries(inputStream);
  }

  private InputStream getInputStreamForQueries() throws FileNotFoundException {
    if (queryConfigFile != null && queryConfigFile.exists()) {
      return new FileInputStream(queryConfigFile);
    }
    return DataExtractionUtils.getInputStreamBasedOnFileName(queryConfigFileName);
  }

  public void readQueries(InputStream stream) throws IOException, SAXException, ParserConfigurationException {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setValidating(false);
    SAXParser parser = factory.newSAXParser();

    parser.parse(stream, new ConfigParserHandler(this));
  }

  public void query() throws IOException, SQLException {
    for (QueryDefinition query : getQueries()) {
      writeToFile(query, dataFrom);
    }
  }

  protected void queryFrom(Date date) throws IOException, SQLException {
    for (QueryDefinition query : getQueries()) {
      writeToFile(query, date);
    }
  }

  protected void writeQueryConfigFile(String fileName) throws IOException {
    FileWriter out = new FileWriter(fileName);
    try {
      writeQueryConfigFileTo(out);
    } finally {
      out.close();
    }
  }

  private void writeQueryConfigFileTo(FileWriter out) throws IOException {
    new ConfigParserHandler(this).writeQueryConfigFileTo(out);
  }

  /**
   * Provides the Query Definitions filtered by the configured CC ccVersion.
   * @return List of QueryDefinition
   */
  public List<QueryDefinition> getQueries() {
    List<QueryDefinition> result = new ArrayList<>();
    for (QueryDefinition query : queries) {
      if (ccVersion == 0 || !query.isExcludedFor(ccVersion)) {
        result.add(query);
      }
    }
    return result;
  }

  public void setQueries(List<QueryDefinition> queries) {
    this.queries = queries;
  }

  public void setDB(String db) {
    if (db != null && db.equals(UNCONFIGED_DB)) {
      _dataExtractorLog.error("The database db field in the config file still contains the default value.  Please check your config file to make sure it has been correctly configured.");
    }
    this.db = db;
  }

  public String getDB() {
    return db;
  }

  public String getConfigFilePath() {
    return configFilePath;
  }

  void setConfigFilePath(String configFilePath) {
    this.configFilePath = configFilePath;
  }

  private String getOutputDir() {
    DataExtractorLog.verifyDirectoryExistsAndIsWritable(outputDir);
    return outputDir;
  }


  public void setOutputDir(String location) {
    DataExtractorLog.verifyDirectoryExistsAndIsWritable(location);
    outputDir = location;
  }


  private boolean isTop() {
    return top;
  }

  protected void setTop() {
    top = true;
  }

  protected ResultSet runQuery(String sql) throws SQLException {
    return runQuery(getDBConnection().prepareStatement(sql));
  }

  public QueryRun runAQuery(PreparedStatement sql) throws SQLException {
    long queryStart = System.currentTimeMillis();
    ResultSet rs = runQuery(sql);
    long queryRunTime = System.currentTimeMillis() - queryStart;
    
    return new QueryRun(rs, queryRunTime);    
  }
  private ResultSet runQuery(PreparedStatement sql) throws SQLException {
    if (!isReadyToQuery()) {
      if (getDBConnection() == null) {
        throw new IllegalStateException("Database, user or password inadequately defined.  Cannot run query");
      }
    }

//    Statement statement = getDBConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

//    display("Running query: " + sql.toString());
    sql.execute();
    return sql.getResultSet();
  }


  private Connection getDBConnection() {
    ensureDBConnection();
    return dbConnection;
  }


  private void ensureDBConnection() {
    try {
      if (dbConnection == null || dbConnection.isClosed()) {
        dbConnection = createDBConnection();
      }
    } catch (SQLException e) {
      _dataExtractorLog.errorToLogMsgToConsole(e, "Error connecting to database. " + getDbConnectionString());
    }
  }


  private Connection createDBConnection() throws SQLException {
//    DriverManager.setLogWriter(new PrintWriter(System.out));
    DriverManager.registerDriver(new OracleDriver());
      DriverManager.registerDriver(new SQLServerDriver());
      DriverManager.registerDriver(new org.postgresql.Driver());

    String url = getDBURL();
    Pattern pattern = Pattern.compile("USERID");
    Matcher matcher = pattern.matcher(url);
    matcher.replaceAll(getDBUserID());

    pattern = Pattern.compile("PASSWORD");
    matcher = pattern.matcher(url);
    matcher.replaceAll(dbUserPassword);
    ensureDriverFor(url);
    return DriverManager.getConnection(url, getDBUserID(), dbUserPassword);
  }

  private String getDbConnectionString() {
    return "url [" + getDBURL() + "] user [" + getDBUserID() + "] password [" + dbUserPassword + "]";
  }

  private void ensureDriverFor(String urlString) throws SQLException {
    String url = urlString.toLowerCase();
    if (DriverManager.getDriver(urlString) != null) {
      return;  // found driver
    }
    Pattern oracle = Pattern.compile(ORACLE);
    Matcher matcher = oracle.matcher(url);
    if (matcher.find()) {
      DriverManager.registerDriver(new OracleDriver());
    }
      Pattern sqlServer = Pattern.compile("sqlserver");
      matcher = sqlServer.matcher(url);
      if (matcher.find()) {
          DriverManager.registerDriver(new SQLServerDriver());
      }
      Pattern postgres = Pattern.compile("postgres");
      matcher = postgres.matcher(url);
      if (matcher.find()) {
          DriverManager.registerDriver(new org.postgresql.Driver());
      }
  }


  public PreparedStatement createPreparedStatement(QueryDefinition queryDefinition) throws SQLException {
    if (queryDefinition.isIncremental() && queryDefinition.getLatestUpdateDate() != null) {
      return queryDefinition.getIncrementalStatement(getDBConnection(), queryDefinition.getLatestUpdateDate(), getDBTag(), getDB());
    }
    return queryDefinition.getTransformedQuery(getDBConnection(), getDBTag(), getDB());
  }


  public String getPreparedStatementQuery(QueryDefinition queryDefinition) throws SQLException {
    if (queryDefinition.isIncremental() && queryDefinition.getLatestUpdateDate() != null) {
      return queryDefinition.getIncrementalSQL(getDBTag(), getDB());
    }
    return queryDefinition.getTransformedSQL(dbTag, db);
  }


  public List<String> getPreparedStatementArguments(QueryDefinition queryDefinition) throws SQLException {
    List<String> results = new ArrayList<>();
    if (queryDefinition.isIncremental() && queryDefinition.getLatestUpdateDate() != null) {
      results.add(queryDateFormatter.format(queryDefinition.getLatestUpdateDate()));
    }
    return results;
  }


  public PreparedStatement createChecksumPreparedStatement(QueryDefinition queryDefinition) throws SQLException {
    return queryDefinition.getChecksumStatement(getDBConnection(), getDBTag(), getDB());
  }


  private boolean needsEarliestDateQuery(QueryDefinition queryDefinition) {
    String table = tableNameMap.get(queryDefinition.getName());
    return queryDefinition.isChunk() && (table != null && "none".equals(table) == false);
  }

  private Date getEarliestDate(QueryDefinition queryDefinition) {
    if (!needsEarliestDateQuery(queryDefinition))
      return null;

    Date result = null;
    ResultSet rs = null;
    try {
      rs = runQuery(getEarliestDateSQL(queryDefinition));
      if (rs.next()) {
        result = rs.getTimestamp(1);
      }
    } catch (SQLException e) {
      _dataExtractorLog.error("Error executing earliest data sql [" + getEarliestDateSQL(queryDefinition) + "]" + e.getLocalizedMessage());
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) { /*ignored*/ }
      }
    }
    return result;
  }

  private String getEarliestDateSQL(QueryDefinition queryDefinition) {
    String table = tableNameMap.get(queryDefinition.getName());
    if (queryDefinition.isChunk() == false || table == null || "none".equals(table)) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    if (SQLSERVER.equals(getDBType())) {
      if (getDB() == null || "".equals(getDB())) {
        _dataExtractorLog.info("Expected a database name while constructing earliest date sql, but none was passed in.");
      } else {
        sb.append("use ");
        sb.append(getDB());
        sb.append(";");
      }
    }
    sb.append("select min(createTime) from ");
    if (ORACLE.equals(getDBType())) {
      if (getDBTag() == null || "".equals(getDBTag())) {
        _dataExtractorLog.info("Expected a dbTag while constructing earliest date sql, but none was passed in.");
      } else {
        sb.append(getDBTag());
      }
    }
    sb.append(table);
    if (SQLSERVER.equals(getDBType())) {
      sb.append(";");
    }

    return sb.toString();
  }

  /**
   * Will attempt to connect to the configured database and run a simple test query to verify that it can connect
   *
   * @return true if successfully connected and ran query
   */
  protected boolean checkDatabase() {
    // Check the db attribute and make sure that it will work.
    String db = getDB();
    if (db == null) {
      _dataExtractorLog.error("The db attribute on the database tag in the configuration file may not be null");
    }
    String dbType = getDBType();
    if (dbType == null) {
      _dataExtractorLog.error("Couldn't ascertain local CC database type.");
      return false;
    }

    if (SQLSERVER.equalsIgnoreCase(dbType)) {
      if ("".equals(db)) {
        _dataExtractorLog.error("For SQLServer databases, the db attribute on the database tag in the config file must contain the name of the CC database from which data will be extracted.");
      }
    }

    Connection conn = null;
    PreparedStatement stmt = null;
    try {
      conn = getDBConnection();
     if (conn == null)
        return false;
      _dataExtractorLog.info("attempting to attach to: " + getDBURL());
      String query = getConnectionTestQuery();
      stmt = conn.prepareStatement(query);
      if (stmt == null)
        return false;
      stmt.execute();
      return true;
    } catch (SQLException e) {
      _dataExtractorLog.info("Got SQLException connection to database");
      return false;
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          _dataExtractorLog.error("Error closing database statement");
        }
      }
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          _dataExtractorLog.error("Error closing database connection");
        }
      }
    }
  }

  private String getConnectionTestQuery() {
    if (ORACLE.equals(getDBType())) {
      return ORACLE_TEST_QUERY;
    }
    if (SQLSERVER.equals(getDBType())) {
      return "select 1";
    }

    return "select 1 from cctl_yesno";
  }

  private boolean successfullyRunsOracleQuery() {
    try {
      ResultSet resultSet = runQuery(ORACLE_TEST_QUERY);
      if (resultSet.next()) {
        return resultSet.getInt(1) == 1;
      }
    } catch (Exception e) {
    }
    return false;
  }

  protected void writeQueryTo(QueryDefinition queryDefinition, Writer writer, Date since) throws SQLException, IOException {
    long start = System.currentTimeMillis();
    int expectedRows = getExpectedRows(queryDefinition, since);
    if (expectedRows != UNKNOWN_ROW_COUNT) {
      _dataExtractorLog.info("Expecting [" + expectedRows + "] rows");
    }
    CSVWriter csvWriter = new CSVWriter(writer);
    writer.append("Expected Rows: " + expectedRows + "\n");
    writer.append("<data start>\n");
    writeHeaderRows(queryDefinition, writer);

    // Check to see if this query needs/can use an earliest date, other than the one in the query definition.
    // This is because we want to continue to chunk even when the earliest date is earlier than the default
    // in the query definition.
    Date earliestDate = queryDefinition.getEarliestDate();
    if (needsEarliestDateQuery(queryDefinition)) {
      Date queriedDate = getEarliestDate(queryDefinition);
      String queriedDateString = "";
      if (queriedDate != null)
        queriedDateString = queryDateFormatter.format(queriedDate);
      _dataExtractorLog.info("Queried for earliest date [" + queriedDateString + "]");
      if (queriedDate != null && queriedDate.before(earliestDate)) {
        _dataExtractorLog.info("Setting earliest date from [" + queryDateFormatter.format(earliestDate) + "] to [" + queriedDateString + "]");
        earliestDate = queriedDate;
      }
    }
    _dataExtractorLog.info("earliest date is [" + queryDateFormatter.format(earliestDate) + "]");

    int row = 0;
    int chunks = 0;
    long queryTime = 0;
    long maxQueryTime = 0;
    PreparedStatement statement = null;
    PreparedStatement nullQuery = null;
    // todo: should switch the chunking to use binding variables, making it easier to deal with dates
    try {
      if (since != null) {
        statement = queryDefinition.getIncrementalStatement(getDBConnection(), since, getDBTag(), getDB());
        _dataExtractorLog.info("Running incremental: " + queryDefinition.getIncrementalSQL(getDBTag(), getDB()));
        QueryRun run = runAQuery(statement);
        _dataExtractorLog.info("Query took [" + run.getTimeToRun() + "] ms");
        queryTime += run.getTimeToRun();
        maxQueryTime = Math.max(maxQueryTime, run.getTimeToRun());
        row = writeResults(run.getResultSet(), csvWriter, queryDefinition, expectedRows, row);
      } else if (queryDefinition.isChunk()) {
        Date later = null;
        Date earlier = getToday();
        int days = -1 * queryDefinition.getDaysForEachChunk();
        boolean queryAgain = true;
        while (queryAgain) {
          if (later != null && later.before(earliestDate)) {
            queryAgain = false;
            earlier = null;
            // Get a new chunked statement.  Since we have no earlier date to limit it, this will be different
            // than the statement we've been using, which took two dates.  Our expectation is that this query will
            // return no results, if the earliest date query ran successfully, since all rows should have a createTime
            // later than the value of later at this point.
            statement = queryDefinition.getChunkedStatement(getDBConnection(), earlier, later, getDBTag(), getDB());
          }

          String chunkedSQL = queryDefinition.getChunkedSQL(earlier, later, getDBTag(), getDB());
          if (statement == null) {
            statement = queryDefinition.getChunkedStatement(getDBConnection(), earlier, later, getDBTag(), getDB());
          } else {
            queryDefinition.setChunkedDates(statement, earlier, later);
          }
          String earlierDate = "";
          if (earlier != null)
            earlierDate = queryDateFormatter.format(earlier);
          String laterDate = "";
          if (later != null)
            laterDate = queryDateFormatter.format(later);
          _dataExtractorLog.info("Running chunk with dates earlier [" + earlierDate + "] later [" + laterDate + "]: " + chunkedSQL);
          QueryRun run = runAQuery(statement);
          _dataExtractorLog.info("Query took [" + run.getTimeToRun() + "] ms");
          queryTime += run.getTimeToRun();
          maxQueryTime = Math.max(maxQueryTime, run.getTimeToRun());
          int prevCount = row;
          row = writeResults(run.getResultSet(), csvWriter, queryDefinition, expectedRows, row);
          if (earlier == null || later == null) {
            statement = null;
          }
          if (prevCount == row) {
            days = days - (1 - (int) (0.1 * days));
          } else {
            _dataExtractorLog.info(row - prevCount + " rows returned");
            days = -1 * queryDefinition.getDaysForEachChunk();
          }
          if (earlier != null) {
            later = earlier;
            earlier = DateUtils.addDays(earlier, days);
          }
          chunks++;
        }
        String nullSQL = queryDefinition.getNullChunkedSQL(getDBTag(), getDB());
        nullQuery = queryDefinition.getNullChunkedQuery(getDBConnection(), getDBTag(), getDB());
        if (nullSQL != null) {
          _dataExtractorLog.info("Running null sql chunk: " + nullSQL);
          QueryRun run = runAQuery(nullQuery);
          _dataExtractorLog.info("Query took [" + run.getTimeToRun() + "] ms");
          queryTime += run.getTimeToRun();
          int prevCount = row;
          row = writeResults(run.getResultSet(), csvWriter, queryDefinition, expectedRows, row);
          _dataExtractorLog.info(row - prevCount + " rows returned");
        }
      } else {
        Pattern selectFound = Pattern.compile("SELECT|select");
        if (!selectFound.matcher(queryDefinition.getTransformedSQL(getDBTag(), getDB())).find()) {
          _dataExtractorLog.info("Problem with query definition: " + queryDefinition.getName());
          _dataExtractorLog.info("original " + queryDefinition.getOriginalSQL());
          _dataExtractorLog.info("transformed " + queryDefinition.getTransformedSQL(getDBTag(), getDB()));
          _dataExtractorLog.info("ccVersion " + queryDefinition.getVersion());
          _dataExtractorLog.info("count " + queryDefinition.getCountSQL(since, getDBTag(), getDB()));
        }
        _dataExtractorLog.info("Running: " + queryDefinition.getTransformedSQL(getDBTag(), getDB()));
        statement = queryDefinition.getTransformedQuery(getDBConnection(), getDBTag(), getDB());
        QueryRun run = runAQuery(statement);
        _dataExtractorLog.info("Query took [" + run.getTimeToRun() + "] ms");
        queryTime += run.getTimeToRun();
        maxQueryTime = queryTime;
        // write the rows
        row = writeResults(run.getResultSet(), csvWriter, queryDefinition, expectedRows, row);
      }
    } finally {
      if (statement != null) statement.close();
      if (nullQuery != null) nullQuery.close();
      csvWriter.flush();
    }
    long runEnd = System.currentTimeMillis();
    //debug("Time to run query: " + queryTime + " seconds");
    writer.append("<data end>\n");
    writer.append("Stats:\n");
    writer.append("Date of Run,Chunks,Total Query Time,Max Query Time,Processing Time,DB Name,Customer Code\n");
    writer.append(DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, Locale.US).format(new Date()) + "," + chunks + "," + queryTime + "," + maxQueryTime + "," + (runEnd - start) / 1000 + "," + getDB() + "," + getCustomerID() + "\n");
    _dataExtractorLog.info("Stats:");
    _dataExtractorLog.info("Date of Run, Chunks, Total Query Time, Max Query Time, Processing Time, DB Name, Customer Code, Query Name");
    _dataExtractorLog.info(DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, Locale.US).format(new Date()) + ", " + chunks + ", " + queryTime + ", " + maxQueryTime +
      ", " + (runEnd - start) / 1000 + ", " + getDB() + ", " + getCustomerID() + ", " + queryDefinition.getName() + "\n");
    //debug("Time to process query and write results: " + (queryEnd - start) / 1000 + " seconds");
    writer.append("Query: " + queryDefinition.getName() + " ccVersion: " + queryDefinition.getVersion() + "\n");
    writer.append(queryDefinition.getTransformedSQL(getDBTag(), getDB()));
    writer.flush();
    csvWriter.close();
    _dataExtractorLog.info("Wrote: " + row + " rows (expected [" + expectedRows + "] rows)\n");
  }

  private Date getToday() {
    return new Date();
  }

  private int writeResults(ResultSet rs, CSVWriter writer, QueryDefinition queryDefinition, int expectedRows, int row) throws SQLException, IOException {
    long startTime = System.currentTimeMillis();
    if (!rs.next()) {
      _dataExtractorLog.info("Total write time [" + (System.currentTimeMillis() - startTime) + "]");
      return row;
    }

    boolean haveRowCount = expectedRows != UNKNOWN_ROW_COUNT;
    int rowsSinceNotification = 0;
    for (; !rs.isAfterLast(); rs.next()) {
      if (rowsSinceNotification++ == ROWS_TO_WRITE_BEFORE_NOTIFICATION) {
        if (haveRowCount) {
          _dataExtractorLog.info("written: " + row + " rows of " + expectedRows);
        } else {
          _dataExtractorLog.info("written: " + row + " rows");
        }
        rowsSinceNotification = 1;
      }
      row++;
      if (isTop() && row > 25) {
        break;
      }

      String[] toWrite = new String[queryDefinition.columns.size()];
      int i = 0;
      for (ColumnDef column : queryDefinition.columns) {
        toWrite[i++] = column.getOutputResults(rs);
      }
      writer.writeNext(toWrite);
    }
    rs.close();
    _dataExtractorLog.info("Total write time [" + (System.currentTimeMillis() - startTime) + "]");
    return row;
  }

  private void writeHeaderRows(QueryDefinition queryDefinition, Writer writer) throws IOException {
    boolean firstColumn = true;
    for (ColumnDef column : queryDefinition.columns) {
      if (firstColumn) {
        firstColumn = false;
      } else {
        writer.append(",");
      }
      writer.append(column.getName());
    }
    writer.append("\n");

    // write column types
    firstColumn = true;
    for (ColumnDef column : queryDefinition.columns) {
      if (firstColumn) {
        firstColumn = false;
      } else {
        writer.append(",");
      }
      writer.append(column.getType());
    }
    writer.append("\n");
  }

  /**
   * Runs the countSQL for the given queryDefinition in an attempt to get an estimate of how many rows will be returned
   * by the main query.  Since this query and the actual will be run at different times, it is possible they will
   * return a different number of rows.
   * @param queryDefinition - the query to be run
   * @param since - a date in case the query is being run incrementally
   * @return the number of rows expected from the main query
   */
  public int getExpectedRows(QueryDefinition queryDefinition, Date since) {
    if (queryDefinition.getCountSQL(since, getDBTag(), getDB()) == null) {
      return UNKNOWN_ROW_COUNT;
    }
    ResultSet count = null;
    PreparedStatement countStatement = null;
    try {
      countStatement = queryDefinition.getCountQuery(getDBConnection(), since, getDBTag(), getDB());
      _dataExtractorLog.info("Running count query: " + queryDefinition.getCountSQL(since, getDBTag(), getDB()));
      QueryRun run = runAQuery(countStatement);
      count = run.getResultSet();
      _dataExtractorLog.info("Count query took [" + run.getTimeToRun() + "] ms");
      if (!count.next()) {
        return UNKNOWN_ROW_COUNT;
      }
      return count.getInt(1);
    } catch (SQLException e) {
      return UNKNOWN_ROW_COUNT;
    } finally {
      try {
        if (count != null) count.close();
        if (countStatement != null) countStatement.close();
      } catch (SQLException e) {
      }
    }
  }

  protected void writeToFile(QueryDefinition queryDefinition, Date since) throws IOException, SQLException {
    Writer writer = null;
    try {
      writer = createOutputFile(queryDefinition);
      writeQueryTo(queryDefinition, writer, since);
      _dataExtractorLog.info(getOutputFile(queryDefinition));
      writer.close();
      writer = null;
      if (verifyFile(queryDefinition, false)) {
        if (!verifyFile(queryDefinition)) {
          _dataExtractorLog.info("The written file has more records than expected.");
        }
      } else {
        _dataExtractorLog.error("The written file does not match the query results and has fewer results than expected");
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  /**
   * Creates a single zip file with <customerID>_results.zip in the working directory with the other query result files
   * contained.
   * @throws IOException
   */
  public void compressResults() throws IOException {
    compressResultsTo(new File(getOutputDir(), getCustomerID() + "_results.zip"));
  }

  /**
   * Writes the query results to the file given in a compressed format.
   * @throws IOException
   */
  private void compressResultsTo(File dest) throws IOException {
    _dataExtractorLog.info("compressing to: " + dest.getPath());
    ZipArchiveOutputStream out = new ZipArchiveOutputStream(dest);
    // UTF-8 is supposed to be the default, but if we don't set it, the data is garbled after unzipping.  So...
    out.setEncoding("UTF-8");
    out.setUseZip64(Zip64Mode.AsNeeded);    // Needs to be 64 bit mode for larger files.  As needed will use 32bit for smaller

    for (QueryDefinition query : getQueries()) {
      String queryResultFileName = getOutputFile(query);
      String zippedFilename = getOutputFilename(query);
      writeToZipStream(out, queryResultFileName, outputDir, zippedFilename);
    }
    _dataExtractorLog.closeLogFile();
    if (_dataExtractorLog.getLogFile().exists() && _dataExtractorLog.getLogFile().length() > 0) {
      writeToZipStream(out, _dataExtractorLog.getLogFile().getPath(), _dataExtractorLog.getLogsDir(), _dataExtractorLog.getLogFile().getName());
    }
    out.close();
//    debug("checksum: " + checksum.getChecksum().getValue());
  }

  private void writeToZipStream(ZipArchiveOutputStream out, String fileToInclude, String zipDirectory, String zippedFilename) throws IOException {
    BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(fileToInclude), BUFFER);
    ZipArchiveEntry entry = new ZipArchiveEntry(zipDirectory + File.separator + zippedFilename);
//    debug("Zipping: " + fileToInclude);
    out.putArchiveEntry(entry);
    int count;
    byte data[] = new byte[BUFFER];
    while ((count = inputStream.read(data, 0, BUFFER)) != -1) {
      out.write(data, 0, count);
    }
    out.closeArchiveEntry();
    inputStream.close();
  }


  protected boolean verifyFiles() throws IOException {
    return verifyFiles(true);
  }

  protected boolean verifyFiles(boolean exactMatchExpected) throws IOException {
    boolean allAreValid = true;
    for (QueryDefinition query : queries) {
      if (!verifyFile(query, exactMatchExpected)) {
        allAreValid = false;
        _dataExtractorLog.info("Invalid results in: " + getOutputFile(query));
      }
    }
    return allAreValid;
  }

  private boolean verifyFile(QueryDefinition query, boolean exactMatchExpected) throws IOException {
    return verifyFile(getOutputFile(query), exactMatchExpected);
  }

  private boolean verifyFile(QueryDefinition query) throws IOException {
    return verifyFile(getOutputFile(query));
  }

  private boolean verifyFile(String outputFile) throws IOException {
    return verifyFile(outputFile, true);
  }

  private boolean verifyFile(String outputFile, boolean exactMatchExpected) throws IOException {
    int lines = -2;  //discount the first to rows as header information
    String[] lastTen = new String[10];
    int lastIndex = 0;

    Pattern start = Pattern.compile("<data start>");
    Pattern end = Pattern.compile("<data end>");
    Pattern expectedRows = Pattern.compile("Expected Rows: (\\d+)");

    FileReader fileReader = new FileReader(outputFile);
    BufferedReader in = new BufferedReader(fileReader);
    String line;
    boolean dataStarted = false;
    boolean showStats = false;
    int linesExpected = 0;
    while ((line = in.readLine()) != null) {
      if (showStats) {
        _dataExtractorLog.info(line);
      } else if (dataStarted) {
        if (end.matcher(line).find()) {
          if (lines != linesExpected) {
            showStats = true;
            continue;
          } else {
            break;
          }
        }
        lines++;
        if (lastIndex == 10) {
          lastIndex = 0;
        }
        lastTen[lastIndex++] = line;
      } else {
        Matcher expectedRowMatch = expectedRows.matcher(line);
        if (expectedRowMatch.find()) {  //Expected Rows:
          linesExpected = new Integer(expectedRowMatch.group(1));
        }
        if (start.matcher(line).find()) {
          dataStarted = true;
        }
      }
    }
    in.close();
    _dataExtractorLog.info("Checking: " + outputFile);
    if (lines != linesExpected) {
      if (exactMatchExpected || lines < linesExpected) {
        _dataExtractorLog.info("Expected: " + linesExpected);
        _dataExtractorLog.info("Read: " + lines);
        _dataExtractorLog.info("Last 10 rows:");
        for (String s : lastTen) {
          _dataExtractorLog.info(s);
        }
        return false;
      }
    }
    return true;
  }


  private Writer createOutputFile(QueryDefinition queryDefinition) throws IOException {
    String fullName = getOutputFile(queryDefinition);
    _dataExtractorLog.info("Writing: " + fullName);
    File outputFile = new File(fullName);
    if (!outputFile.exists()) {
      _dataExtractorLog.info("Need to create: " + outputFile.getAbsolutePath());
    }
    if (!outputFile.getParentFile().exists()) {
      _dataExtractorLog.info("Need to create dir: " + outputFile.getParentFile().getAbsolutePath());
      _dataExtractorLog.info(outputFile.getParentFile().mkdir() ? "made dir" : "failed");
      if (!outputFile.getParentFile().exists()) {
        _dataExtractorLog.info("still missing" + outputFile.getParentFile().getAbsolutePath());
      }
    }
    FileOutputStream fos = new FileOutputStream(outputFile);
    OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
    return new BufferedWriter(osw);
  }

  private String getOutputFile(QueryDefinition queryDefinition) throws IOException {
    File dir = new File(getOutputDir());
    if (queryDefinition.getName() == null) {
      throw new IllegalStateException("Customer ID and Query name null");
    }
    String fileName = getOutputFilename(queryDefinition);
    return dir.getAbsoluteFile() + File.separator + fileName;
  }

  private String getOutputFilename(QueryDefinition queryDefinition) {
    return getCustomerID() + "_" + queryDefinition.getName() + ".csv";
  }


  public String getClient() {
    return client;
  }


  void setClient(String client) {
    this.client = client;
  }


  /**
   * QueryRun is a wrapper containing the results of a query as well as meta information about the run, specifically
   * the amount of time it took to run
   */
  public class QueryRun {
    ResultSet resultSet;
    long timeToRun;

    private QueryRun(ResultSet rs, long queryRunTime) {
      resultSet = rs;
      timeToRun = queryRunTime;
    }

    public ResultSet getResultSet() {
      return resultSet;
    }

    public long getTimeToRun() {
      return timeToRun;
    }
  }
}
