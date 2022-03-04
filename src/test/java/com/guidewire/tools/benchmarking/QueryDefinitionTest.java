package com.guidewire.tools.benchmarking;

import java.io.File;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import com.guidewire.util.TestUtil;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups="unit")
public class QueryDefinitionTest {

  public void testTransformUsingPropertiesFile() throws Exception {
    QueryDefinition query = new QueryDefinition();
    query.setOriginalSQL("&SELECT &DATE(clm.lossdate, lossdate), &DATE(clm.reportdate, reportdate) FROM cc_claim clm&EMPTY");

    Properties properties = new Properties();
    properties.load(getClass().getClassLoader().getResourceAsStream("testSubstitution.properties"));
    query.transform(properties);
    Assert.assertEquals(query.getTransformedSQL("", ""),
            "SELECT CONVERT(VARCHAR(23), clm.lossdate, 120) lossdate, CONVERT(VARCHAR(23), clm.reportdate, 120) reportdate FROM cc_claim clm");
  }


  public void testGetColumnDefs() {
    QueryDefinition query = new QueryDefinition();
    query.setOriginalSQL("&SELECT &INTEGER(int, myInt), &STRING(string, stringAs), &DATE(dt, dateAs), &DECIMAL(dec, decimalAs), &DATETIME(clm.lossdate, lossdate) &FROM cc_claim clm&EMPTY");

    List<ColumnDef> columns = query.getColumnDefs();
    List<ColumnDef> expected = new ArrayList<>(4);
    expected.add(ColumnDef.createDefinition(ColumnDef.INTEGER, "myInt"));
    expected.add(ColumnDef.createDefinition(ColumnDef.STRING, "stringAs"));
    expected.add(ColumnDef.createDefinition(ColumnDef.DATE, "dateAs"));
    expected.add(ColumnDef.createDefinition(ColumnDef.DECIMAL, "decimalAs"));
    expected.add(ColumnDef.createDefinition(ColumnDef.DATETIME, "lossdate"));
    Assert.assertEquals(expected, columns);
  }

  public void testGetColumnDefsWithCarriageReturnBetweenSelectAndFrom() {
    QueryDefinition query = new QueryDefinition();
    query.setOriginalSQL("&SELECT &INTEGER(int, myInt), \n" +
      "&STRING(string, stringAs), \n" +
      "&DECIMAL(dec, decimalAs), \n" +
      "&DATETIME(clm.lossdate, lossdate)\n" +
      "&FROM cc_claim clm&EMPTY");

    List<ColumnDef> columns = query.getColumnDefs();
    List<ColumnDef> expected = new ArrayList<>(4);
    expected.add(ColumnDef.createDefinition(ColumnDef.INTEGER, "myInt"));
    expected.add(ColumnDef.createDefinition(ColumnDef.STRING, "stringAs"));
    expected.add(ColumnDef.createDefinition(ColumnDef.DECIMAL, "decimalAs"));
    expected.add(ColumnDef.createDefinition(ColumnDef.DATETIME, "lossdate"));
    Assert.assertEquals(expected, columns);
  }

  public void testGetColumnDefsWithBracketTypes() {
    QueryDefinition query = new QueryDefinition();
    query.setOriginalSQL("&SELECT &INTEGER_BRACKET[int, myInt], \n" +
      "&STRING_BRACKET[string, stringAs], \n" +
      "&DECIMAL_BRACKET[dec, decimalAs], \n" +
      "&DATETIME_BRACKET[clm.lossdate, lossdate]\n" +
      "&FROM cc_claim clm&EMPTY");

    List<ColumnDef> columns = query.getColumnDefs();
    List<ColumnDef> expected = new ArrayList<>(4);
    expected.add(ColumnDef.createDefinition(ColumnDef.INTEGER, "myInt"));
    expected.add(ColumnDef.createDefinition(ColumnDef.STRING, "stringAs"));
    expected.add(ColumnDef.createDefinition(ColumnDef.DECIMAL, "decimalAs"));
    expected.add(ColumnDef.createDefinition(ColumnDef.DATETIME, "lossdate"));
    Assert.assertEquals(expected, columns);
  }

  public void testGetColumnDefsThrowsWithMissingSelectKeyword() {
    QueryDefinition query = new QueryDefinition();
    query.setOriginalSQL("&BAD x &FROM cc_claim clm&EMPTY");

    try {
      query.getColumnDefs();
      Assert.fail("Shouldn't have parsed");
    } catch (QueryDefinition.MalformedSelectException e) {
      // Expected
    }
  }

  public void testGetColumnDefsThrowsWithMissingFromKeyword() {
    QueryDefinition query = new QueryDefinition();
    query.setOriginalSQL("&SELECT x &BAD cc_claim clm&EMPTY");

    try {
      query.getColumnDefs();
      Assert.fail("Shouldn't have parsed");
    } catch (QueryDefinition.MalformedSelectException e) {
      // Expected
    }
  }

  public void testGetColumnDefsThrowsWithInvalidDataType() {
    QueryDefinition query = new QueryDefinition();
    query.setOriginalSQL("&SELECT &BAD(int, myInt), &STRING(string, stringAs), &DECIMAL(dec, decimalAs), &DATETIME(clm.lossdate, lossdate) &FROM cc_claim clm&EMPTY");

    try {
      query.getColumnDefs();
      Assert.fail("Should have choked on unrecognized 'BAD' datatype");
    } catch (ColumnDef.UnknownColumnTypeException e) {
      // Expected
    }
  }

  public void testCanWriteXMLFile() throws Exception {
    DataExtractor extractor = new DataExtractor();
    String configFile = "query_config.xml";
    String rewrittenFile = "query_config_processed.xml";
    extractor.setQueryConfigFileName(configFile);
    extractor.readQueries();
    extractor.writeQueryConfigFile(rewrittenFile);

    DataExtractor checking = new DataExtractor();
    checking.setQueryConfigFile(new File(rewrittenFile));
    checking.readQueries();
    QueryDefinition expected = extractor.getQueries().get(0);
    QueryDefinition actual = checking.getQueries().get(0);
    Assert.assertEquals(expected.getName(), actual.getName());
    Assert.assertEquals(expected.getColumns(), actual.getColumns());
  }

  public void testDefaultDate() {
    QueryDefinition query = new QueryDefinition();
    DateFormat formatter = DateFormat.getDateInstance(DateFormat.SHORT);
    Assert.assertEquals(formatter.format(query.getEarliestDate()), "1/1/00");
  }

  public void testChunkedSQLSubstitution() throws Exception {
    QueryDefinition query = new QueryDefinition();
    DateFormat formatter = DateFormat.getDateInstance(DateFormat.SHORT);
    Date start = formatter.parse("1/1/12");
    Date end = formatter.parse("2/2/12");

    query.setOriginalSQL("&CHUNK_START &CHUNK_END &DB_TAG");
    Assert.assertEquals(query.getChunkedSQL(start, end, "dbtag", ""), "'01/01/2012' '02/02/2012' dbtag");
    
    query.setOriginalSQL("SELECT var1 var1 \n" +
            "var2 var2, \n" +
            "var3 var3 \n" +
            "FROM cc_claim clm \n" +
            "WHERE clm.createDate BETWEEN &CHUNK_START AND &CHUNK_END");
    String chunkedSQL = query.getChunkedSQL(start, end, "", "");
    Assert.assertEquals(chunkedSQL, "SELECT var1 var1 \n" +
            "var2 var2, \n" +
            "var3 var3 \n" +
            "FROM cc_claim clm \n" +
            "WHERE clm.createDate BETWEEN '01/01/2012' AND '02/02/2012'");
  }

  public void testChunkedSQLNullSubstitution() {
    QueryDefinition query = new QueryDefinition();

    query.setOriginalSQL("&CHUNK_SQL_START(trn.createtime > CONVERT(datetime, '1/24/12', 1)) &CHUNK_SQL_END(trn.createtime <= CONVERT(datetime, '1/24/12', 1)) &DB_TAG");
    Assert.assertEquals(query.getChunkedSQL(null, null, "dbtag", ""), "  dbtag");

  }
  
  public void testChunkedSQLRemovesNullCase() throws Exception {
    QueryDefinition query = new QueryDefinition();
    DateFormat formatter = DateFormat.getDateInstance(DateFormat.SHORT);
    Date start = formatter.parse("1/1/12");
    Date end = formatter.parse("2/2/12");

    query.setOriginalSQL("&CHUNK_START &CHUNK_END &CHUNK_SQL_NULL(trn.createdate = NULL) &DB_TAG");
    Assert.assertEquals(query.getChunkedSQL(start, end, "dbtag", ""), "'01/01/2012' '02/02/2012'  dbtag");

    query.setOriginalSQL("SELECT var1 var1 \n" +
            "var2 var2, \n" +
            "var3 var3 \n" +
            "FROM cc_claim clm \n" +
            "WHERE \n" +
            "&CHUNK_SQL_NULL(clm.createdate = NULL) \n" +
            "clm.createDate BETWEEN &CHUNK_START AND &CHUNK_END");
    String chunkedSQL = query.getChunkedSQL(start, end, "", "");
    Assert.assertEquals(chunkedSQL, "SELECT var1 var1 \n" +
            "var2 var2, \n" +
            "var3 var3 \n" +
            "FROM cc_claim clm \n" +
            "WHERE \n" +
            " \nclm.createDate BETWEEN '01/01/2012' AND '02/02/2012'");
  }

  public void testNullChunkedSQL() {
    QueryDefinition query = new QueryDefinition();

    query.setOriginalSQL("&CHUNK_SQL_NULL(trn.createtime = NULL) &DB_TAG");
    Assert.assertEquals(query.getNullChunkedSQL("dbtag", ""), "trn.createtime = NULL dbtag");
  }

  public void testDB_TAGSubstitution() {
    QueryDefinition query = new QueryDefinition();

    query.setOriginalSQL("&DB_TAGBeginning");
    Assert.assertEquals(query.getTransformedSQL("This is the", ""), "This is theBeginning");

    query.setOriginalSQL("This is the &DB_TAG");
    Assert.assertEquals(query.getTransformedSQL("end", ""), "This is the end");

    query.setOriginalSQL("This is the \n" +
            "&DB_TAG \n" +
            "&DB_TAG \n" +
            "end");
    Assert.assertEquals(query.getTransformedSQL("middle", ""), "This is the \nmiddle \nmiddle \nend");
  }

  public void testIncrementalSQLRemovesChunking() throws Exception {
    QueryDefinition query = new QueryDefinition();
    DateFormat formatter = DateFormat.getDateInstance(DateFormat.SHORT);
    formatter.parse("1/1/12");

    query.setOriginalSQL("&CHUNK_SQL_START(trn.createtime > CONVERT(datetime, '1/24/12', 1)) \n" +
            "&CHUNK_SQL_NULL(null sql here ) \n" +
            "&CHUNK_SQL_END(trn.createtime <= CONVERT(datetime, '1/24/12', 1)) \n" +
            " &SINCE_SQL(exposure.updatetime > ?) &DB_TAG");

    Assert.assertEquals(query.getIncrementalSQL("dbtag", ""), " \n \n \n exposure.updatetime > ? dbtag");
  }

  public void testLakeOnly() throws Exception {
    Assert.assertTrue(TestUtil.getQueryDefinition("lakeOnly_default").isLakeOnly());
    Assert.assertTrue(TestUtil.getQueryDefinition("lakeOnly_true").isLakeOnly());
    Assert.assertFalse(TestUtil.getQueryDefinition("lakeOnly_false").isLakeOnly());
  }

  public void testWillExcludeQueriesBasedOnVersion() throws Exception {
    DataExtractor extractor = new DataExtractor();
    extractor.setQueryConfigFileName(TestUtil.QUERY_CONFIG_FILE);
    extractor.setCCVersion("4");
    extractor.readQueries();
    Assert.assertNull(TestUtil.getQueryDefinition(extractor.getQueries(), "exclude_cc4"));
    Assert.assertNotNull(TestUtil.getQueryDefinition(extractor.getQueries(), "lakeOnly_default")); // no exclusions

    extractor = new DataExtractor();
    extractor.setQueryConfigFileName(TestUtil.QUERY_CONFIG_FILE);
    extractor.setCCVersion("5");
    extractor.readQueries();
    Assert.assertNotNull(TestUtil.getQueryDefinition(extractor.getQueries(), "exclude_cc4"));

    extractor = new DataExtractor();
    extractor.setQueryConfigFileName(TestUtil.QUERY_CONFIG_FILE);
    extractor.setCCVersion("cc4");
    extractor.readQueries();
    Assert.assertNull(TestUtil.getQueryDefinition(extractor.getQueries(), "exclude_cc4"));

    extractor = new DataExtractor();
    extractor.setQueryConfigFileName(TestUtil.QUERY_CONFIG_FILE);
    extractor.setCCVersion("cc6");
    extractor.readQueries();
    QueryDefinition queryDefinition = TestUtil.getQueryDefinition(extractor.getQueries(), "exclude_several");
    Assert.assertTrue(queryDefinition.isExcludedFor(4));
    Assert.assertTrue(queryDefinition.isExcludedFor(5));
    Assert.assertTrue(queryDefinition.isExcludedFor(8));
    Assert.assertTrue(queryDefinition.isExcludedFor(10));
    Assert.assertFalse(queryDefinition.isExcludedFor(6));
    Assert.assertFalse(queryDefinition.isExcludedFor(11));
  }
}