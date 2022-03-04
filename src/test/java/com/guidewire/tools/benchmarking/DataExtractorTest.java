package com.guidewire.tools.benchmarking;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;


@Test(groups="unit")
public class DataExtractorTest {
  private static final String SIMPLE_BENCHMARKING = "test_config.xml";
  private static final String TEST_CONFIG_CC6_MSSQL_XML = "test_config_cc6_mssql.xml";
  private static final String TEST_CONFIG_CC6_ORACLE_XML_DBTAG = "test_config_cc6_oracle_dbtag.xml";
  private static final String TEST_CONFIG_CC6_ORACLE_XML_DBTAG_NODOT = "test_config_cc6_oracle_dbtag_nodot.xml";
  private static final String TEMPORARY_WORKING_DIR = "tempWorkingDir";

  public void ensureWorkingDir() throws Exception {
    File tempDir = new File(TEMPORARY_WORKING_DIR, "bogusFile");
    if (!tempDir.getParentFile().exists()) {
      System.out.println("Creating directories " + tempDir.getAbsolutePath());
      System.out.println("Creating directories " + tempDir.getCanonicalPath());
      System.out.println("current dir " + getCurrentDirectory());
      tempDir.mkdirs();
    }
  }

  @AfterMethod(timeOut=5000)
  public void tearDown() {
    System.out.println("removing directories!!!");
    removeWorkingDir();
    System.out.println("Done removing directories!!!");
  }

  public void removeWorkingDir() {
    File tempDir = new File(TEMPORARY_WORKING_DIR);
    wipeOutDirectory(tempDir);
    tempDir = new File(getCurrentDirectory(), TEMPORARY_WORKING_DIR);
    wipeOutDirectory(tempDir);
  }
  
  private void wipeOutDirectory(File parent) {
    if (parent.exists()) {
      if (parent.isDirectory()) {
        for (File child : parent.listFiles()) {
          wipeOutDirectory(child);
        }
      }
      System.out.println("Deleting: " + parent.getAbsolutePath());
      parent.delete();
    }
  }

  @Test(timeOut = 10000, enabled=false)
  public void testCanMakeDBConnection() throws Exception {
    DataExtractor extractor = createDBReadyExtractor();

    ResultSet resultSet = extractor.runQuery("use edge_benchmark_cc6; \nselect COUNT(*) as count from cc_user");
    resultSet.next();
    Assert.assertEquals(resultSet.getInt("count"), 161);
  }

  @Test(timeOut = 10000, enabled=false)
  public void testCanWriteToStream() throws Exception {
    String foobar = System.getProperty("jenkinsSysVartest");
    System.out.print("******************** ");
    System.out.print("jenkins:" + foobar);
    System.out.print(" ********************\n");

    DataExtractor extractor = createDBReadyExtractor();

    QueryDefinition queryDefinition = createUserQueryDefinition();
    StringWriter writer = new StringWriter();

    extractor.writeQueryTo(queryDefinition, writer, null);

    Assert.assertTrue(writer.getBuffer().toString().contains("10"));
    System.out.print(writer.getBuffer().toString());
  }

  @Test(timeOut = 10000, enabled=false)
  public void testCanWriteToFile() throws Exception {
    String currentDirectory = getCurrentDirectory();
    File expected = new File(currentDirectory, "acme_user.csv");

    DataExtractor extractor = createDBReadyExtractor();
    QueryDefinition queryDefinition = createUserQueryDefinition();
    extractor.setOutputDir(expected.getParentFile().getAbsolutePath());
    extractor.writeToFile(queryDefinition, null);

    Assert.assertTrue(expected.exists(), "should exist " + expected.getAbsolutePath());
    Assert.assertTrue(expected.length() > 0, "length should be greater than zero " + expected.getAbsolutePath());
    long now = Calendar.getInstance().getTimeInMillis();
    Assert.assertTrue(expected.lastModified() > now - 2000, "should have been created recently " + (expected.lastModified() - now) + expected.getAbsolutePath());
  }

  @Test(timeOut = 10000, enabled=false)
  public void testCanReadXMLConfig() throws Exception {
    DataExtractor extractor = new DataExtractor(SIMPLE_BENCHMARKING, "", DataExtractionRunner.PHRASE);
    Assert.assertEquals("DatabaseTag.", extractor.getDBTag());
    Assert.assertEquals("working", extractor.getDataExtractorLog().getLogsDir());
    Assert.assertEquals("TestCustomer", extractor.getUsername());
    Assert.assertEquals("http://URL", extractor.getUploadURL());
    ResultSet resultSet = extractor.runQuery("use edge_benchmark_cc6; select COUNT(*) as count from cc_user");
    resultSet.next();
    Assert.assertEquals(resultSet.getInt("count"), 161);
  }


  @Test(timeOut = 10000)
  public void testCanReadXMLConfigWithSQL() throws Exception {
    File file = new File(".", ".");
    System.out.println(file.getAbsolutePath());
    System.out.println(file.getCanonicalPath());
    DataExtractor extractor = new DataExtractor(SIMPLE_BENCHMARKING, "", DataExtractionRunner.PHRASE);
    extractor.readQueries();
    Assert.assertTrue(extractor.getQueries().size() > 0);
    QueryDefinition queryDefinition = extractor.getQueries().get(0);
    Assert.assertTrue(queryDefinition.getOriginalSQL().contains("select publicID"));
    Assert.assertTrue(queryDefinition.getOriginalSQL().contains("convert(varchar(23), updateTime, 120) as updateTime,"));
    Assert.assertTrue(queryDefinition.getOriginalSQL().contains("convert(varchar(23), CreateTime, 120) as createTime"));
    Assert.assertTrue(queryDefinition.getOriginalSQL().contains("from cc_user"));
    Assert.assertTrue(queryDefinition.isCatchUp());
    Assert.assertEquals(queryDefinition.getName(), "user");
    Assert.assertEquals(4, queryDefinition.getColumns().size());
  }

  @Test(timeOut = 10000)
  public void testWillExcludeWholeQueriesBasedOnVersion() throws Exception {
    DataExtractor extractor = new DataExtractor();
    extractor.setQueryConfigFileName("version_filtering_query_config.xml");
    extractor.setCCVersion("4");
    extractor.readQueries();
    Assert.assertEquals(extractor.getQueries().size(), 1);
    extractor.writeQueryConfigFile("working/filtering_write_test.xml");
  }

  // The next two tests, testDBTag() and testDBTagNoDot() check that we do the correct
  // transform on the db_tag property.  We test with db_tag properties both with and without
  // a trailing . on the value.
  @Test(timeOut = 10000)
  public void testDBTag() throws Exception {
    DataExtractor extractor = new DataExtractor(TEST_CONFIG_CC6_ORACLE_XML_DBTAG, "", DataExtractionRunner.PHRASE);
    extractor.setQueryConfigFileName("query_config_transaction.xml");
    extractor.readQueries();
    QueryDefinition queryDefinition = extractor.getQueries().get(0);
    // This isn't the greatest test, since the query we're getting is in the form that it would be read into
    // the server, that is, it's without the transformations that should be done on the server.  But it will
    // work for this test, at least.
    String query = queryDefinition.getTransformedSQL(extractor.getDBTag(), "oracle");
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cc_transaction"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cc_transactionlineitem"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cctl_transaction"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cctl_transactionstatus"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cctl_transactionlifecyclestate"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cctl_costtype"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cctl_costcategory"));
  }

  @Test(timeOut = 10000)
  public void testDBTagNoDot() throws Exception {
    DataExtractor extractor = new DataExtractor(TEST_CONFIG_CC6_ORACLE_XML_DBTAG_NODOT, "", DataExtractionRunner.PHRASE);
    extractor.setQueryConfigFileName("query_config_transaction.xml");
    extractor.readQueries();
    QueryDefinition queryDefinition = extractor.getQueries().get(0);
    // This isn't the greatest test, since the query we're getting is in the form that it would be read into
    // the server, that is, it's without the transformations that should be done on the server.  But it will
    // work for this test, at least.
    String query = queryDefinition.getTransformedSQL(extractor.getDBTag(), "oracle");
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cc_transaction"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cc_transactionlineitem"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cctl_transaction"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cctl_transactionstatus"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cctl_transactionlifecyclestate"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cctl_costtype"));
    Assert.assertTrue(query.contains("edge_benchmark_cc6.cctl_costcategory"));
  }

  private String getCurrentDirectory() {
    return System.getProperty("user.dir");
  }


  @Test(enabled=false)
  public void testOracleConnection() throws Exception {
    DriverManager.setLogWriter(new PrintWriter(System.out));
    DataExtractor extractor = createOracleReadyExtractor();
    ResultSet resultSet = extractor.runQuery("select COUNT(*) as count from cc_user");
    resultSet.next();
    Assert.assertEquals(resultSet.getInt("count"), 161);
  }

  @Test(enabled=false)
  public void testCheckConnectionSQLServer() {
    DataExtractor extractor = createDBReadyExtractor();
    Assert.assertTrue(extractor.checkDatabase());
  }

  @Test(enabled=false)
  public void testCheckConnectionOracle() {
    DataExtractor extractor = createOracleReadyExtractor();
    Assert.assertTrue(extractor.checkDatabase());
  }

  public void testCheckConnectionFailOnEmpty() {
    DataExtractor extractor = new DataExtractor();
    extractor.setDBURL("");
    extractor.setDBUserPassword("");
    Assert.assertFalse(extractor.checkDatabase());
  }

  @Test(enabled=false)
  public void testCanGetCount() {
    DataExtractor extractor = createDBReadyExtractor();
    QueryDefinition queryDefinition = createUserQueryDefinition();
    Assert.assertEquals(extractor.getExpectedRows(queryDefinition, null), 161);
  }

  @Test(enabled=false)
  public void testIncrementalQuery() throws Exception {
    DataExtractor extractor = new DataExtractor(TEST_CONFIG_CC6_MSSQL_XML, "", DataExtractionRunner.PHRASE);
    File expected = new File(getCurrentDirectory(), "test_financials.csv");
    extractor.setOutputDir(expected.getParentFile().getAbsolutePath());
    extractor.setQueryConfigFileName("query_cc6_sqlserver.xml");
    extractor.readQueries();
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

    extractor.queryFrom(format.parse("20120101"));

    Assert.assertTrue(expected.exists());
    Assert.assertTrue(expected.length() > 0);
    Assert.assertEquals(linecount(expected), 97);
    Assert.assertTrue(extractor.verifyFiles());
  }

  @Test(enabled=false)
  public void testVerifyFileFewerRowsThanExpected() throws Exception {
    DataExtractor extractor = new DataExtractor(TEST_CONFIG_CC6_MSSQL_XML, "", DataExtractionRunner.PHRASE);
    File expected = new File(getCurrentDirectory(), "test_financials.csv");
    extractor.setOutputDir(expected.getParentFile().getAbsolutePath());
    extractor.setQueryConfigFileName("query_cc6_sqlserver.xml");
    extractor.readQueries();

    // Set the count to expect an additional row
    extractor.getQueries().get(0).setCountSQL("use &DB_NAME;\n" +
            "     SELECT COUNT(trn.id) + COUNT(trn.id) from cc_transaction trn\n" +
            "     &SINCE_SQL( WHERE trn.updatetime > ?)");
    extractor.query();

    Assert.assertFalse(extractor.verifyFiles());
    Assert.assertFalse(extractor.verifyFiles(false));
  }

  @Test(enabled=false)
  public void testVerifyFileMoreRowsThanExpected() throws Exception {
    DataExtractor extractor = new DataExtractor(TEST_CONFIG_CC6_MSSQL_XML, "", DataExtractionRunner.PHRASE);
    File expected = new File(getCurrentDirectory(), "test_financials.csv");
    extractor.setOutputDir(expected.getParentFile().getAbsolutePath());
    extractor.setQueryConfigFileName("query_cc6_sqlserver.xml");
    extractor.readQueries();

    // Set the count to expect an additional row
    extractor.getQueries().get(0).setCountSQL("use &DB_NAME;\n" +
            "     SELECT COUNT(trn.id)/COUNT(trn.id) from cc_transaction trn\n" +
            "     &SINCE_SQL( WHERE trn.updatetime > ?)");
    extractor.query();

    Assert.assertFalse(extractor.verifyFiles());
    Assert.assertTrue(extractor.verifyFiles(false));
  }

  private DataExtractor createDBReadyExtractor() {
    DataExtractor extractor = new DataExtractor();
    extractor.setDBURL("jdbc:sqlserver://devdb2\\SQLSERVER2008R2:2009");
    extractor.setDBUserID("sa");
    extractor.setDBUserPassword("Gw_123");
    extractor.setDB("edge_benchmark_cc6");
    extractor.setDBType("sqlserver");
    extractor.setUsername("acmeupload");
    return extractor;
  }

  private DataExtractor createOracleReadyExtractor() {
    DataExtractor extractor = new DataExtractor();
    extractor.setDBURL("jdbc:oracle:thin:USERID/PASSWORD@devdb2:1525/gwCtr11g");
    extractor.setDBUserID("edge_benchmark_cc6");
    extractor.setDBUserPassword("cc");
    extractor.setDBType("oracle");
    return extractor;
  }

  private static QueryDefinition createUserQueryDefinition() {
    QueryDefinition queryDefinition = new QueryDefinition();
    queryDefinition.setOriginalSQL("use &DB_NAME; \nSELECT publicID, integerExt, convert(varchar(23), updateTime, 120) as updateTime, convert(varchar(23), CreateTime, 120) as createTime FROM cc_user");
    queryDefinition.setCountSQL("use &DB_NAME; \nSELECT count(id) FROM cc_user");
    queryDefinition.columns.add(ColumnDef.createDefinition(ColumnDef.STRING, "publicID"));
    queryDefinition.columns.add(ColumnDef.createDefinition(ColumnDef.INTEGER, "integerExt"));
    queryDefinition.columns.add(ColumnDef.createDefinition(ColumnDef.DATETIME, "updateTime"));
    queryDefinition.columns.add(ColumnDef.createDefinition(ColumnDef.DATETIME, "createTime"));
    queryDefinition.setName("user");
    return queryDefinition;
  }

  private int linecount(File file) throws IOException {
    int lines = 0;
    FileReader fileReader = new FileReader(file);
    BufferedReader in = new BufferedReader(fileReader);
    String line = in.readLine();
    while (line != null) {
      lines++;
      line = in.readLine();
    }
    in.close();
    return lines;
  }

}
