package com.guidewire.cloudviewer.datamoving;

import com.guidewire.cloudviewer.datamoving.client.DataExtractorClient;
import com.guidewire.tools.benchmarking.CSVFileColumnDef;
import com.guidewire.tools.benchmarking.CSVFileDefinition;
import com.guidewire.tools.benchmarking.ColumnDef;
import com.guidewire.tools.benchmarking.DataExtractor;
import com.guidewire.tools.benchmarking.QueryDefinition;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Class description...
 */
@Test(groups="unit")
public class PolicyCSVFileDataSourceTest {
  private static final String YYYY_MM_DD = "yyyy-MM-dd";
  transient SimpleDateFormat YearMonthDateFormatter = new SimpleDateFormat(YYYY_MM_DD);


  public void testGetQueryDefinition() throws Exception {
    CustomerCSVFileDataSource source = getTestPoliciesSource();
    QueryDefinition queryDefinition = source.getQueryDefinition();
    Assert.assertEquals(queryDefinition.getName(), source.getName());
    Assert.assertEquals(queryDefinition.getName(), "policy");
  }

  public void testColumnTypes() throws Exception {
    CustomerCSVFileDataSource source = getTestPoliciesSource();
    List<ColumnDef> columns = source.getColumns();
    Assert.assertEquals(columns.size(), 20);
    Assert.assertEquals(columns.get(1).getType(), ColumnDef.STRING);
    Assert.assertEquals(columns.get(2).getType(), ColumnDef.INTEGER);
    Assert.assertEquals(columns.get(4).getType(), ColumnDef.DATETIME);
    Assert.assertEquals(columns.get(7).getType(), ColumnDef.STRING);
  }

  public void testReadEmptyFile() throws Exception {
    CustomerCSVFileDataSource source = getEmptyPoliciesSource();
    int i = 0;
    while (source.next()) {
      i++;
    }
    Assert.assertEquals(0, i);
  }

  public void testNext() throws Exception {
    CustomerCSVFileDataSource source = getTestPoliciesSource();
    int i = 0;
    while (source.next()) {
      i++;
    }
    Assert.assertEquals(i, 34);
  }

  public void testGets() throws Exception {
    CustomerCSVFileDataSource source = getTestPoliciesSource();
    source.next();
    Assert.assertEquals(source.getString("policyNumber"), "50015");
    Assert.assertEquals(source.getString("addressLine1"), "232 EVERSYDE WAY SW");
    Assert.assertEquals(source.getString("LOB"), "Residential");
    Assert.assertNotNull(source.getDate("policyEffectiveDate"));
  }

  public void testReorderedColumns() throws Exception {
    CustomerCSVFileDataSource source = getReorderedColumnsPoliciesSource();
    source.next();
    Assert.assertEquals(source.getString("policyNumber"), "50015");
    Assert.assertEquals(source.getString("addressLine1"), "232 EVERSYDE WAY SW");
    Assert.assertEquals(source.getString("LOB"), "Residential");
    Assert.assertNotNull(source.getDate("policyEffectiveDate"));
  }

  public void testSourceFileWithByteOrderMark() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getTestInputStream(getTestPoliciesWithBOM()), extractor, getTestCSVFileDefinition());
    assert result.successfullyReadHeaders();
  }

  // This test reads a policy file with some extra header lines.  I believe they are part of an example file
  // we send people, so if we get a file with those headers back, we must be able to read it.
  public void testExtraHeaderLines() throws Exception {
    CustomerCSVFileDataSource source = getExtraHeaderPoliciesSource();
    DataExtractorClient client = new DataExtractorClient(source.getExtractor(), 0, 0);
    QueryResult result = client.buildResultPackage(source);
    List<QueryResult.ResultRow> resultRows = result.getRows();
    Assert.assertEquals(1, result.getRowCount());
    List<String> results = resultRows.get(0).getResults();
    Assert.assertEquals("FL245502333", results.get(1));
    Assert.assertEquals("300 A Philip Randolph Boulevard", results.get(5));
    Assert.assertEquals("Actual Policies", results.get(14));
    Assert.assertEquals("32202", results.get(8));
    String truncatedDate = results.get(3).substring(0,8);

    Date expectedDate = YearMonthDateFormatter.parse("2013-08-08");
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    Date gotDate = format.parse(truncatedDate);
    Assert.assertEquals(expectedDate, gotDate);
    Assert.assertEquals(results.get(12), "");

    List<String> errorMessages = source.getExtractor().getDataExtractorLog().getErrorMessages();
    Assert.assertEquals(2, errorMessages.size());
  }

  // Tests that we can get a data source and convert it to a QueryResult object
  public void testQueryResult() throws Exception {
    CustomerCSVFileDataSource source = getTestPoliciesSource();
    DataExtractorClient client = new DataExtractorClient(source.getExtractor(), 0, 0);
    QueryResult result = client.buildResultPackage(source);

    // Now check the QueryResult object
    Assert.assertEquals(34, result.getRowCount());
    List<ColumnDef> columns = result.getColumns();
    Assert.assertEquals(20, columns.size());
    List<QueryResult.ResultRow> resultRows = result.getRows();
    // Some checks on the first row
    QueryResult.ResultRow firstRow = resultRows.get(0);
    Assert.assertEquals("policyNumber", columns.get(1).getName());
    Assert.assertEquals("50015", firstRow.getResults().get(1));
    Assert.assertEquals("postalCode", columns.get(8).getName());
    Assert.assertEquals("T2Y4V5", firstRow.getResults().get(8));
    Assert.assertEquals("LOB", columns.get(14).getName());
    Assert.assertEquals("Residential", firstRow.getResults().get(14));

    // Some checks on the sixth row, which has some quoted data in it
    QueryResult.ResultRow sixthRow = resultRows.get(5);
    Assert.assertEquals("3412 BRENNER DRIVE N.W., CALGARY, ALBERTA", sixthRow.getResults().get(5));
    Assert.assertEquals("LOT 26, BLOCK 2, PLAN 8018 J.K.", sixthRow.getResults().get(7));
    Assert.assertEquals("T2L1X7", sixthRow.getResults().get(8));
  }

  public void testBadData() throws Exception {
    CustomerCSVFileDataSource source = getBadDataPoliciesSource();
    DataExtractorClient client = new DataExtractorClient(source.getExtractor(), 0, 0);
    QueryResult result = client.buildResultPackage(source);

    // Check the error messages
    List<String> errors = source.getExtractor().getDataExtractorLog().getErrorMessages();
    long rowsProcessed = source.currentRow;
    Assert.assertEquals(4, errors.size());
    Assert.assertTrue(errors.get(0).contains("Row [1]  contains the wrong number of fields; expected: [15] got [16]; "));
    Assert.assertTrue(errors.get(1).contains("Unparseable date: \"Not a date\""));
    Assert.assertTrue(errors.get(2).contains("Unparseable number: \"xxxxxx\""));
    Assert.assertTrue(errors.get(3).contains("Row [8]  contains the wrong number of fields; expected: [15] got [14]"));

    // The QueryResult object should contain the rows that successfully parsed, so check that
    Assert.assertEquals(6, result.getRowCount());
    List<QueryResult.ResultRow> resultRows = result.getRows();
    QueryResult.ResultRow firstRow = resultRows.get(0);
    Assert.assertEquals("P116100", firstRow.getResults().get(1));
    Assert.assertEquals("MN", firstRow.getResults().get(9));
    Assert.assertEquals("Homeowners", firstRow.getResults().get(14));

    QueryResult.ResultRow lastRow = resultRows.get(5);
    Assert.assertEquals("P140100", lastRow.getResults().get(1));
    Assert.assertEquals("1093 Clark Street", lastRow.getResults().get(5));
    // We're going to whack off the hours, minutes, seconds, ms, and time zone, so as
    // to get rid of the time zone, since we don't want the test to be dependent on what
    // time zone it's run in.
    String truncatedDate = lastRow.getResults().get(3).substring(0,8);
    Assert.assertEquals("20070227", truncatedDate);
    // rowsProcessed should equal the number of rows in the file
    Assert.assertEquals(11, rowsProcessed);
  }

  public void testMismatchedQuoteData() throws Exception {
    CustomerCSVFileDataSource source = getMismatchedQuoteSource();
    DataExtractorClient client = new DataExtractorClient(source.getExtractor(), 0, 0);
    client.buildResultPackage(source);

    List<String> errors = source.getExtractor().getDataExtractorLog().getErrorMessages();
    Assert.assertEquals(1, errors.size());
    Assert.assertTrue(errors.get(0).contains("current row is [2], this row has an open quote without a corresponding close quote: "));
    Assert.assertTrue(errors.get(0).contains("java.lang.RuntimeException: java.io.IOException: (startline 3) EOF reached before encapsulated token finished"));
  }

  public void testMismatchedQuote2Data() throws Exception {
    CustomerCSVFileDataSource source = getMismatchedQuote2Source();
    DataExtractorClient client = new DataExtractorClient(source.getExtractor(), 0, 0);
    QueryResult result = client.buildResultPackage(source);

    // This test does not generate any errors even though the data is an example of mismatched
    // quotes.  In this case the quotes are such that a whole line is removed by two different
    // sets of mismatched quotes, and it just happens that we get rows that successfully pass
    // the parsing of all the columns.  This isn't great, but I don't see a way for us to recognize
    // and flag the error involved.
    Assert.assertEquals(3, result.getRowCount());
    List<QueryResult.ResultRow> resultRows = result.getRows();
    Assert.assertTrue(resultRows.get(1).getResults().get(5).startsWith("101 Garfield Avenue"));
    Assert.assertTrue(resultRows.get(2).getResults().get(5).startsWith("1093 Clark Street"));
    List<String> errors = source.getExtractor().getDataExtractorLog().getErrorMessages();
    Assert.assertEquals(2, errors.size());
    Assert.assertTrue(errors.get(0).contains("invalid char between encapsulated token and delimiter"));
    Assert.assertTrue(errors.get(1).contains("Row [2]  contains the wrong number of fields; expected: [15] got [11]; at least one column is missing from this record"));
    Assert.assertTrue(errors.get(1).contains("[70 Pearl Street, , Buffalo, 14202, NY, US, , , Homeowners, , ]"));
  }

  public void testMismatchedQuote3Data() throws Exception {
    CustomerCSVFileDataSource source = getMismatchedQuote3Source();
    DataExtractorClient client = new DataExtractorClient(source.getExtractor(), 0, 0);
    QueryResult result = client.buildResultPackage(source);

    Assert.assertEquals(3, result.getRowCount());

    List<String> errors = source.getExtractor().getDataExtractorLog().getErrorMessages();
    Assert.assertEquals(2, errors.size());
    Assert.assertTrue(errors.get(0).contains("Row may contain mismatched quotes"));
    Assert.assertTrue(errors.get(1).contains("Row [2]  contains the wrong number of fields; expected: [15] got [16]; it may have an unquoted field containing a comma or have mismatched quotes"));
    Assert.assertTrue(errors.get(1).contains("[P124200, 100006, 2007-02-20, 2008-02-20, 295 Main Street,,Buf..., 170 Pearl Street, , Buffalo, 14202, NY, US, , , Homeowners, , ]"));
  }

  public void testMismatchedQuote4Data() throws Exception {
    CustomerCSVFileDataSource source = getMismatchedQuote4Source();
    DataExtractorClient client = new DataExtractorClient(source.getExtractor(), 0, 0);
    QueryResult result = client.buildResultPackage(source);

    Assert.assertEquals(5, result.getRowCount());
    List<QueryResult.ResultRow> resultRows = result.getRows();
    Assert.assertTrue(resultRows.get(1).getResults().get(5).startsWith("295 Main Street"));
    Assert.assertTrue(resultRows.get(2).getResults().get(5).startsWith("170 Pearl Street"));

    List<String> errors = source.getExtractor().getDataExtractorLog().getErrorMessages();
    Assert.assertEquals(0, errors.size());
  }

  public void testMissingRequiredFieldsData() throws Exception {
    CustomerCSVFileDataSource source = getMissingRequiredFieldsSource();
    DataExtractorClient client = new DataExtractorClient(source.getExtractor(), 0, 0);
    QueryResult result = client.buildResultPackage(source);
    Assert.assertEquals(0, result.getRowCount());

    List<String> errors = source.getExtractor().getDataExtractorLog().getErrorMessages();
    for (String error : errors) {
      System.out.println(error);
    }
    Assert.assertEquals(6, errors.size());
    int errorCount = 0;
    List<String> errorStrings = getTestStringsForMissingRequiredFields(true);
    for (String error : errors) {
      Assert.assertTrue(error.contains(errorStrings.get(errorCount++)));
    }
  }

  public void testBadDataFile() throws Exception {
    CSVFileDefinition testCSVFileDefinition = getTestCSVFileDefinition();
    CustomerCSVFileDataSource source = getDataSourceWithBadFileOption(new File(getClass().getClassLoader().getResource(getBadPolicyData()).getFile()), testCSVFileDefinition);
    int policyNumberIndex = testCSVFileDefinition.getColumnIndexForColumnName("PolicyNumber");
    DataExtractorClient client = new DataExtractorClient(source.getExtractor(), 0, 0);
    client.buildResultPackage(source);

    List<String> errors = source.getExtractor().getDataExtractorLog().getErrorMessages();
    for (String error : errors) {
      System.out.println(error);
    }
    List<String> badDataStrings = readDataFromBadFile(getBadDataFile(getBadPolicyData()));
    badDataStrings.remove(0); // remove the header row from the bad data
    badDataStrings = buildBadDataPolicyNumberList(policyNumberIndex, badDataStrings);
    validateErrorDataToBadFile(errors, badDataStrings);
  }

  private List<String> buildBadDataPolicyNumberList(int policyNumberIndex, List<String> dataStrings) {
    List<String> policyNumbers = new ArrayList<>(dataStrings.size());
    for (String dataString : dataStrings) {
      String[] split = dataString.split(",");
      policyNumbers.add(split[policyNumberIndex]);
    }
    return policyNumbers;
  }

  private void validateErrorDataToBadFile(List<String> errors, List<String> badDataStrings) {
    List<String> extractorErrors = new ArrayList<>(errors.size());
    extractorErrors.addAll(errors);
    for (String badFileError : badDataStrings) {
      boolean found = false;
      for (String extractorError : extractorErrors) {
        if(extractorError.contains(badFileError)) {
          found = true;
          break;
        }
      }
      if (!found) {
        Assert.fail("Failed to find bad data errorString in extractor error list : " + badFileError);
      }
    }
    for (String extractorError : errors) {
      boolean found = false;
      if (extractorError.startsWith("Error handling data source row")) {
        for (String badFileError : badDataStrings) {
          if(extractorError.contains(badFileError)) {
            found = true;
            break;
          }
        }
        if (!found) {
          Assert.fail("Failed to find extracter errorString in bad data error list : " + extractorError);
        }
      }
    }
  }

  private List<String> readDataFromBadFile(File badPolicyData) {
    try {
      BufferedReader reader = new BufferedReader(new FileReader(badPolicyData));
      List<String> dataStrings = new ArrayList<>();
      String line;
      while ((line = reader.readLine()) != null) {
        dataStrings.add(line);
      }
      return dataStrings;
    } catch (IOException e) {
      Assert.fail("Unable to read bad file : " + badPolicyData);
    }
    Assert.fail("Failed to read bad file : " + badPolicyData);
    return null;
  }
  //////////////////////////////////////////////////////////////////////////////////

  private CustomerCSVFileDataSource getTestPoliciesSource() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getTestInputStream(getTestPolicies()), extractor, getTestCSVFileDefinition());
    assert result.successfullyReadHeaders();
    return result;
  }

  private CustomerCSVFileDataSource getEmptyPoliciesSource() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getTestInputStream(getEmptyPolicies()), extractor, getTestCSVFileDefinition());
    assert result.successfullyReadHeaders();
    return result;
  }

  private CustomerCSVFileDataSource getExtraHeaderPoliciesSource() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getTestInputStream(getExtraHeaderLinePolicies()), extractor, getTestCSVFileDefinition());
    assert result.successfullyReadHeaders();
    return result;
  }

  private CustomerCSVFileDataSource getReorderedColumnsPoliciesSource() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getTestInputStream(getReorderedColumnsPolicies()), extractor, getTestCSVFileDefinition());
    assert result.successfullyReadHeaders();
    return result;
  }

  private CustomerCSVFileDataSource getBadDataPoliciesSource() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getTestInputStream(getBadPolicyData()), extractor, getTestCSVFileDefinition());
    assert result.successfullyReadHeaders();
    return result;
  }

  private CustomerCSVFileDataSource getMismatchedQuoteSource() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getTestInputStream(getMismatchedQuoteData()), extractor, getTestCSVFileDefinition());
    assert result.successfullyReadHeaders();
    return result;
  }

  private CustomerCSVFileDataSource getMismatchedQuote2Source() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getTestInputStream(getMismatchedQuoteData2()), extractor, getTestCSVFileDefinition());
    assert result.successfullyReadHeaders();
    return result;
  }

  private CustomerCSVFileDataSource getMismatchedQuote3Source() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getTestInputStream(getMismatchedQuoteData3()), extractor,getTestCSVFileDefinition());
    assert result.successfullyReadHeaders();
    return result;
  }

  private CustomerCSVFileDataSource getMismatchedQuote4Source() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getTestInputStream(getMismatchedQuoteData4()), extractor, getTestCSVFileDefinition());
    assert result.successfullyReadHeaders();
    return result;
  }

  private CustomerCSVFileDataSource getMissingRequiredFieldsSource() throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(getBadDataMissingRequiredFields(), extractor, getTestCSVFileDefinition(true));
    assert result.successfullyReadHeaders();
    return result;
  }

  private CustomerCSVFileDataSource getDataSourceWithBadFileOption(File inputFile, CSVFileDefinition testCSVFileDefinition) throws IOException {
    DataExtractor extractor = new DataExtractor();
    CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(inputFile, extractor, testCSVFileDefinition, true);
    assert result.successfullyReadHeaders();
    return result;
  }

  private InputStream getTestInputStream(String filename) {
    return getClass().getClassLoader().getResourceAsStream(filename);
  }

  private File getBadDataFile(String policyFile) {
    try {
      return new File(getClass().getClassLoader().getResource(policyFile).getFile().concat(".bad"));
    } catch (NullPointerException e) {
      Assert.fail("Unable to create bad data file for : " + policyFile);
    }
    return null;
  }

  private String getTestPolicies() {
    return "test_policy.csv";
  }

  private String getTestPoliciesWithBOM() {
    return "test_bomheader.csv";
  }

  private String getEmptyPolicies() {
    return "test_policy_empty.csv";
  }

  private String getExtraHeaderLinePolicies() {
    return "extraHeaderPolicyTest.csv";
  }

  private String getReorderedColumnsPolicies() {
    return "test_policy_columns_reordered.csv";
  }

  private String getBadPolicyData() {
    return "badDataPolicyTest.csv";
  }

  private String getMismatchedQuoteData() {
    return "badDataMismatchedQuotesPolicyTest.csv";
  }

  private String getMismatchedQuoteData2() {
    return "badDataMismatchedQuotes2.csv";
  }

  private String getMismatchedQuoteData3() {
    return "badDataMismatchedQuotes3.csv";
  }

  private String getMismatchedQuoteData4() {
    return "badDataMismatchedQuotes4.csv";
  }

  private InputStream getBadDataMissingRequiredFields() {
    return new ByteArrayInputStream(createTestDataForMissingRequiredFields().getBytes());
    //return getClass().getClassLoader().getResourceAsStream("badDataMissingRequiredFields.csv");
  }

  private CSVFileDefinition getTestCSVFileDefinition() {
    return getTestCSVFileDefinition(false);
  }

  private CSVFileDefinition getTestCSVFileDefinition(boolean requireExtraFields) {
    List<CSVFileColumnDef> columnDefs = new ArrayList<>();
    CSVFileColumnDef.ColumnStatus extraFieldsStatus = CSVFileColumnDef.ColumnStatus.NOTREQUIRED;
    if (requireExtraFields) {
      extraFieldsStatus = CSVFileColumnDef.ColumnStatus.REQUIRED;
    }
    String idList = "policyid";
    columnDefs.add(new CSVFileColumnDef("id", "INTEGER", idList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String numberList = "policynumber";
    columnDefs.add(new CSVFileColumnDef("policyNumber", "STRING", numberList, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String locIdList = "policylocationid";
    columnDefs.add(new CSVFileColumnDef("locationId", "INTEGER", locIdList, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String policyEffectiveDateList = "policyeffectivestartdate";
    columnDefs.add(new CSVFileColumnDef("policyEffectiveDate", "DATETIME", policyEffectiveDateList, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String policyExpirationDateList = "policyeffectiveenddate";
    columnDefs.add(new CSVFileColumnDef("policyExpirationDate", "DATETIME", policyExpirationDateList, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String addLine1List ="policyaddressline1";
    columnDefs.add(new CSVFileColumnDef("addressLine1", "STRING", addLine1List, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String addLine2List = "policyaddressline2";
    columnDefs.add(new CSVFileColumnDef("addressLine2", "STRING", addLine2List, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String cityList = "policycity";
    columnDefs.add(new CSVFileColumnDef("city", "STRING", cityList, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String postalCodeList = "policypostalcode";
    columnDefs.add(new CSVFileColumnDef("postalCode", "STRING", postalCodeList, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String stateList = "policystate";
    columnDefs.add(new CSVFileColumnDef("state", "STRING", stateList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String countyList = "policycounty";
    columnDefs.add(new CSVFileColumnDef("county", "STRING", countyList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String countryList = "policycountry";
    columnDefs.add(new CSVFileColumnDef("country", "STRING", countryList, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String latitudeList = "policylatitude";
    columnDefs.add(new CSVFileColumnDef("latitude", "DECIMAL", latitudeList, extraFieldsStatus));

    String longitudeList = "policylongitude";
    columnDefs.add(new CSVFileColumnDef("longitude", "DECIMAL", longitudeList, extraFieldsStatus));

    String LOBList = "policylineofbusiness";
    columnDefs.add(new CSVFileColumnDef("LOB", "STRING", LOBList, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String createTimeList = "createdate/time";
    columnDefs.add(new CSVFileColumnDef("createTime", "STRING", createTimeList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String updateTimeList = "updatedate/time";
    columnDefs.add(new CSVFileColumnDef("updateTime", "STRING", updateTimeList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String TIVList = "policytotalinsuredvalue";
    columnDefs.add(new CSVFileColumnDef("total_insured_value", "STRING", TIVList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String annualPremiumList = "policyannualpremium";
    columnDefs.add(new CSVFileColumnDef("annual_premium", "STRING", annualPremiumList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String policyInceptionDateList = "policyinceptiondatetime";
    columnDefs.add(new CSVFileColumnDef("policyInceptionDate", "DATE", policyInceptionDateList, extraFieldsStatus));

    return new CSVFileDefinition("policy", columnDefs);
  }

  /*******************Test Data**************************************************************************************************************************************/

  private String createTestDataForMissingRequiredFields() {
    List<String> testStringsForMissingRequiredRows = getTestStringsForMissingRequiredFields(false);
    StringBuilder missingRequiredFieldsStrings = new StringBuilder();
    for (String testString : testStringsForMissingRequiredRows) {
            missingRequiredFieldsStrings.append(testString);
    }
    return missingRequiredFieldsStrings.toString();
  }

  // Most of the tests in this class rely on bad data files kept in the resources directory. Because the missing required fields test needs to test that each specific data type
  // generates correct error messages if field data is missing, it is clearer to construct the bad data in this method. Here we can label the bad data with description variable
  // names so it is obvious what is being tested in each row. We also have a corresponding error string for each row of bad data.
  private List<String> getTestStringsForMissingRequiredFields(boolean errorStrings) {
    String headerRow = "PolicyNumber,LocationID,Policy Effective Start Date,Policy Effective End Date,Address Line 1,AddressLine2,Policy City,Policy Postal Code,Policy State,Policy Country,Latitude,Longitude,LOB,Create Date/Time,Update Date/Time, Policy Inception Date\n";
    String missingStringRequiredField = "P116700,100003,2007-02-06,2008-02-06,,,Mississauga,L4W 4Y2,ON,CA,32.0000,33.000000,Homeowners,,,2007-02-06\\n\n";
    String missingDateRequiredField = "P124100,100005,2007-02-15,2008-02-15,5275 Edina Industrial Boulevard,,Edina,55439,MN,US,32.0000,33.000000,Homeowners,,,\n";
    String missingTimestampRequiredField = "P124100,100005,,2008-02-15,44 Hawley Street,,Binghamton,13901,NY,US,32.0000,33.000000,Homeowners,,,2007-02-06\n";
    String missingIntRequiredField = "P116100,,2007-02-05,2008-02-05,5275 Edina Industrial Boulevard,,Edina,55439,MN,US,32.0000,33.000000,Homeowners,,,2007-02-06\n";
    String missingDecimalRequiredField = "P128100,100007,2007-02-22,2008-02-22,170 Pearl Street,,Buffalo,14202,NY,US,,33.000000,Homeowners,,,2007-02-06\n";
    String missingMultipleRequiredFields = ",100000,2007-01-31,2008-01-31,1001 E. Hillsdale Blvd.,Suite 800,Foster City,94404,CA,US,,,Homeowners,,,2007-02-06\n";

    String missingStringRequiredFieldError = "Null value found for required field addressLine1 of type STRING";
    String missingDateRequiredFieldError = "Null value found for required field policyInceptionDate of type DATE";
    String missingTimestampRequiredFieldError = "Null value found for required field policyEffectiveDate of type DATETIME";
    String missingIntRequiredFieldError = "Null value found for required field locationId of type INTEGER";
    String missingDecimalRequiredFieldError = "Null value found for required field latitude of type DECIMAL";
    String missingMultipleRequiredFieldsError = "column [latitude], error is [Null value found for required field latitude of type DECIMAL]," +
            " column [longitude], error is [Null value found for required field longitude of type DECIMAL]";

    List<String> testData = new ArrayList<>();
    if (!errorStrings) {
      testData.add(headerRow);
      testData.add(missingStringRequiredField);
      testData.add(missingDateRequiredField);
      testData.add(missingTimestampRequiredField);
      testData.add(missingIntRequiredField);
      testData.add(missingDecimalRequiredField);
      testData.add(missingMultipleRequiredFields);
    }   else {
      testData.add(missingStringRequiredFieldError);
      testData.add(missingDateRequiredFieldError);
      testData.add(missingTimestampRequiredFieldError);
      testData.add(missingIntRequiredFieldError);
      testData.add(missingDecimalRequiredFieldError);
      testData.add(missingMultipleRequiredFieldsError);
    }
    return testData;
  }


}
