package com.guidewire.cloudviewer.datamoving.client;

import com.guidewire.tools.benchmarking.CSVFileColumnDef;
import com.guidewire.tools.benchmarking.CSVFileDefinition;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for the customer csv file definition serialization and deserialization. Customer csv file definitions are stored on
 * the DES and sent to the client in serialized form. This class tests that the definitions are serialized and deserialized correctly.
 */
@Test(groups="unit")
public class DeserializeCSVFileDefinitionTest {

  public void testDeserializeCSVFileDefinition() {
    CSVFileDefinition testCSVFileDefinition = getTestCSVFileDefinition(null);
    String jsonString = testCSVFileDefinition.getAsJSON();
    List<CSVFileDefinition> csvFileDefinitions  = null;
    try {
      csvFileDefinitions = CSVFileDefinition.convertResponseBodyToCSVFileDefinitions(new ByteArrayInputStream(jsonString.getBytes()));
    } catch (Exception e) {
      Assert.fail("Failed to deserialize CSVFileDefinitions: " + e.toString());
    }
    Assert.assertNotNull(csvFileDefinitions);
    Assert.assertEquals(csvFileDefinitions.size(), 1);
    Assert.assertTrue(testCSVFileDefinition.equals(csvFileDefinitions.get(0)));
  }

  public void testDeserializeCSVFileDefinitionWithDateFormat () {
    String dateFormatForCreateDate = "MM/dd/yyyy HH:mm:ss aaa";
    CSVFileDefinition testCSVFileDefinition = getTestCSVFileDefinition(dateFormatForCreateDate);
    String jsonString = testCSVFileDefinition.getAsJSON();
    List<CSVFileDefinition> csvFileDefinitions  = null;
    try {
      csvFileDefinitions = CSVFileDefinition.convertResponseBodyToCSVFileDefinitions(new ByteArrayInputStream(jsonString.getBytes()));
    } catch (Exception e) {
      Assert.fail("Failed to deserialize CSVFileDefinitions: " + e.toString());
    }
    Assert.assertNotNull(csvFileDefinitions);
    Assert.assertEquals(csvFileDefinitions.size(), 1);
    Assert.assertTrue(testCSVFileDefinition.equals(csvFileDefinitions.get(0)));
  }

  private CSVFileDefinition getTestCSVFileDefinition(String dateFormatForCreateDate) {
    List<CSVFileColumnDef> columnDefs = new ArrayList<CSVFileColumnDef>();
    String idList = "policyid";
    columnDefs.add(new CSVFileColumnDef("id", "INTEGER", idList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String numberList = "policynumber";
    columnDefs.add(new CSVFileColumnDef("policyNumber", "STRING", numberList, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String locIdList = "policylocationid";
    columnDefs.add(new CSVFileColumnDef("locationId", "STRING", locIdList, CSVFileColumnDef.ColumnStatus.REQUIRED));

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
    columnDefs.add(new CSVFileColumnDef("latitude", "DECIMAL", latitudeList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String longitudeList = "policylongitude";
    columnDefs.add(new CSVFileColumnDef("longitude", "DECIMAL", longitudeList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String LOBList = "policylineofbusiness";
    columnDefs.add(new CSVFileColumnDef("LOB", "STRING", LOBList, CSVFileColumnDef.ColumnStatus.REQUIRED));

    String createTimeList = "CreatedDate";
    columnDefs.add(new CSVFileColumnDef("createTime", "DATETIME", dateFormatForCreateDate, createTimeList,
            CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String updateTimeList = "updatedate/time";
    columnDefs.add(new CSVFileColumnDef("updateTime", "STRING", updateTimeList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String TIVList = "policytotalinsuredvalue";
    columnDefs.add(new CSVFileColumnDef("total_insured_value", "STRING", TIVList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String annualPremiumList = "policyannualpremium";
    columnDefs.add(new CSVFileColumnDef("annual_premium", "STRING", annualPremiumList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    String policyInceptionDateList = "policyinceptiondatetime";
    columnDefs.add(new CSVFileColumnDef("policyInceptionDate", "DATETIME", policyInceptionDateList, CSVFileColumnDef.ColumnStatus.NOTREQUIRED));

    return new CSVFileDefinition("policy", columnDefs);
  }
}
