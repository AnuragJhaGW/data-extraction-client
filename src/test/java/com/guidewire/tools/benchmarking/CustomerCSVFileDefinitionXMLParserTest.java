package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.CustomerCSVFileDataSource;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.List;

/**
 * Test the parsing of CustomerCSVFileDefinition config files
 */
@Test(groups="unit")
public class CustomerCSVFileDefinitionXMLParserTest {
  public void testReadFileDefinitionFromXML() throws Exception {
    CSVFileDefinition fileDefinition = readFileDefFromXMLFile("base_csvfiledefinition.xml");
    String jsonString = fileDefinition.getAsJSON();
    List<CSVFileDefinition> csvFileDefinitions  = CSVFileDefinition.convertResponseBodyToCSVFileDefinitions(new ByteArrayInputStream(jsonString.getBytes()));
    Assert.assertNotNull(csvFileDefinitions);
    Assert.assertEquals(csvFileDefinitions.size(), 1);
    Assert.assertTrue(fileDefinition.equals(csvFileDefinitions.get(0)));
  }

  public void testApplyCustomerDiffsFromXMLFileUpdatesFileDefinition() throws Exception {
    CSVFileDefinition baseFileDefinition = readFileDefFromXMLFile("base_csvfiledefinition.xml");
    CSVFileDefinitionModifier customerFileDef = new CSVFileDefinitionModifier(readFileDefFromXMLFile("customer_csvfiledefinition.xml"));
    CSVFileDefinition modifiedFileDef = customerFileDef.applyTo(baseFileDefinition);
    Assert.assertTrue(modifiedFileDef.getColumnDefForColumnName("latitude").isOmitted());
    Assert.assertTrue(modifiedFileDef.getColumnDefForColumnName("longitude").isOmitted());
    modifiedFileDef.removeOmittedColumns();
    Assert.assertNull(modifiedFileDef.getColumnDefForColumnName("latitude"));
    Assert.assertNull(modifiedFileDef.getColumnDefForColumnName("longitude"));
  }

  public void testCorrectyReadDataHeaderWithCustomerSpecificAliases() throws Exception {
    String customerFileDefString = "<csvFileDefinitions> " +
      "<csvfiledef datatablename=\"policy\" version=\"1.0\"  > " +
          "<csvcolumn name=\"latitude\" type=\"DECIMAL\" alias=\"pol_lat\" columnstatus=\"not_required\"/> " +
          "<csvcolumn name=\"longitude\" type=\"DECIMAL\" alias=\"pol_long\" columnstatus=\"not_required\"/>  " +
      "</csvfiledef> " +
    "</csvFileDefinitions>";

    String latLongAliasData = "PolicyNumber,LocationID,Policy Effective Start Date,Policy Effective End Date,Address Line 1,AddressLine2,Policy City,Policy Postal Code,Policy State,Policy Country,pol_lat,pol_long,LOB,Create Date/Time,Update Date/Time\n" +
            "P110600,100000,2007-01-31,2008-01-31,1001 E. Hillsdale Blvd.,Suite 800,Foster City,94404,CA,US,,,Homeowners,,\n" +
            "P116100,100001,2007-02-05,2008-02-05,5275 Edina Industrial Boulevard,,Edina,55439,MN,US,,,Homeowners,,\n";

    CSVFileDefinition baseFileDefinition = readFileDefFromXMLFile("base_csvfiledefinition.xml");
    CSVFileDefinition customerFileDefinition = readFileDefFromString(customerFileDefString);
    CSVFileDefinitionModifier fileDefinitionModifier = new CSVFileDefinitionModifier(customerFileDefinition);
    CSVFileDefinition modifiedFileDef = fileDefinitionModifier.applyTo(baseFileDefinition);
    Assert.assertEquals(customerFileDefinition.getColumnDefForColumnName("latitude").getAlias(), modifiedFileDef.getColumnDefForColumnName("latitude").getAlias());
    Assert.assertEquals(customerFileDefinition.getColumnDefForColumnName("longitude").getAlias(), modifiedFileDef.getColumnDefForColumnName("longitude").getAlias());
    InputStream dataInputStream = new  ByteArrayInputStream(latLongAliasData.getBytes());
    try {
      CustomerCSVFileDataSource customerCSVFileDataSource = new CustomerCSVFileDataSource(dataInputStream, new DataExtractor(), modifiedFileDef);
      Assert.assertTrue(customerCSVFileDataSource.successfullyReadHeaders());
    } catch (Exception e) {
      Assert.fail("Got exception reading data file " + e.getMessage());
    }
  }

  public void testCorrectlyReadsInputFileHeaderAfterApplyingCustomerOmits () throws Exception {
    CSVFileDefinition baseFileDefinition = readFileDefFromXMLFile("base_csvfiledefinition.xml");
    CSVFileDefinitionModifier customerFileDef = new CSVFileDefinitionModifier(readFileDefFromXMLFile("customer_csvfiledefinition.xml"));
    CSVFileDefinition modifiedFileDef = customerFileDef.applyTo(baseFileDefinition);
    DataExtractor extractor = new DataExtractor();
    String filename = "customerPolicyTestWithLatLongOmitted.csv";
    URL resource = getClass().getClassLoader().getResource(filename);
    try {
      CustomerCSVFileDataSource result = new CustomerCSVFileDataSource(new File(resource.getFile()),
            extractor, modifiedFileDef);
      Assert.assertTrue(result.successfullyReadHeaders());
    } catch (IOException e) {
      Assert.fail("IOException while reading input file");
    }
  }

  public void testCustomerOmitOfRequiredColumnFails() throws Exception {
    String customerFileDefString = "<csvFileDefinitions> " +
      "<csvfiledef datatablename=\"policy\" version=\"1.0\"  > " +
          "<csvcolumn name=\"policynumber\" type=\"DECIMAL\" alias=\"policynumber\" columnstatus=\"omitted\" /> " +
      "</csvfiledef> " +
    "</csvFileDefinitions>";

    CSVFileDefinition baseFileDefinition = readFileDefFromXMLFile("base_csvfiledefinition.xml");
    CSVFileDefinitionModifier customerFileDef = new CSVFileDefinitionModifier(readFileDefFromString(customerFileDefString));
    try {
      customerFileDef.applyTo(baseFileDefinition);
      Assert.fail("A column cannot be both omitted and required");
    } catch (InvalidParameterException e) {
      Assert.assertEquals("A required column cannot be changed to not required or omitted status", e.getMessage());
    }
  }

  public void testGetAsXMLReturnsCorrectXML() throws Exception {
    CSVFileDefinition baseFileDefinition = readFileDefFromXMLFile("base_csvfiledefinition.xml");
    String asXML = baseFileDefinition.getAsXML();
    CSVFileDefinition fileDefinitionFromXML = readFileDefFromString(asXML);
    Assert.assertTrue(baseFileDefinition.equals(fileDefinitionFromXML));
  }

  public void testGetAsXMLReturnsCorrectXMLForDateFormat() throws Exception {
    CSVFileDefinition baseFileDefinition = readFileDefFromXMLFile("dateformat_csvfiledefinition.xml");
    String asXML = baseFileDefinition.getAsXML();
    CSVFileDefinition fileDefinitionFromXML = readFileDefFromString(asXML);
    Assert.assertTrue(baseFileDefinition.equals(fileDefinitionFromXML));
  }

  private CSVFileDefinition readFileDefFromXMLFile(String filename) throws Exception {
    InputStream inputStream = getTestInputStream(filename);
    return getCsvFileDefinition(inputStream);
  }

  private CSVFileDefinition readFileDefFromString(String fileDefString) throws Exception {
    InputStream inputStream = new  ByteArrayInputStream(fileDefString.getBytes());
    return getCsvFileDefinition(inputStream);
  }

  private CSVFileDefinition getCsvFileDefinition(InputStream inputStream) throws Exception {
    return CSVFileDefinition.convertXMLToFileDefinition(inputStream, new DataExtractor());
/*
    try {
      return CSVFileDefinition.convertXMLToFileDefinition(inputStream, new DataExtractor());
    } catch (Exception e) {
      Assert.fail("Failed to construct file definition " + e.getMessage());
    }
    return null;
*/
  }

  private InputStream getTestInputStream(String filename) {
    return getClass().getClassLoader().getResourceAsStream(filename);
  }
}
