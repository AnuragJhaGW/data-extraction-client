package com.guidewire.cloudviewer.datamoving;

import com.guidewire.tools.benchmarking.ColumnDef;
import com.guidewire.tools.benchmarking.QueryDefinition;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Class description...
 */
@Test(groups="unit")
public class CSVFileDataSourceTest {

  public void testGetQueryDefinition() throws Exception {
    String exposure = "exposure";
    CSVFileDataSource source = getTestExposureSource(exposure);
    QueryDefinition queryDefinition = source.getQueryDefinition();
    Assert.assertEquals(queryDefinition.getName(), source.getName());
    Assert.assertEquals(queryDefinition.getName(), exposure);
  }

  public void testGetColumns() throws Exception {
    CSVFileDataSource source = getTestExposureSource("exposure");
    List<ColumnDef> columns = source.getColumns();
    Assert.assertEquals(columns.size(), 16);
    Assert.assertEquals(columns.get(0).getType(), ColumnDef.ID);
    Assert.assertEquals(columns.get(3).getType(), ColumnDef.STRING);
    Assert.assertEquals(columns.get(4).getType(), ColumnDef.DATETIME);
    Assert.assertEquals(columns.get(6).getType(), ColumnDef.DATE);
    Assert.assertEquals(columns.get(8).getType(), ColumnDef.BIT);
  }

  public void testReadEmptyFile() throws Exception {
    CSVFileDataSource source = getEmptyExposureSource("empty");
    int i = 0;
    while (source.next()) {
      i++;
    }
    Assert.assertEquals(0, i);
  }

  public void testNext() throws Exception {
    CSVFileDataSource source = getTestExposureSource("exposure");
    int i = 0;
    while (source.next()) {
      i++;
    }
    Assert.assertEquals(i, 25);
  }

  public void testGets() throws Exception {
    CSVFileDataSource source = getTestExposureSource("exposure");
    source.next();
    Assert.assertEquals(source.getString("publicID"), "demo_sample:10001");
    Assert.assertEquals(source.getDouble("loss"), new Double(1.5));
    Assert.assertEquals(source.getInt("otherCoverage"), new Integer(0));
  }

  public void testGetDate() throws Exception {
    CSVFileDataSource source = getTestExposureSource("exposure");
    source.next();
    source.next();  // go to the second row
    Date updateTime = source.getDate("updateTime");
    Assert.assertNull(updateTime);
    Date createTime = source.getTimestamp("createTime");
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSSZ");
    Assert.assertEquals(format.format(createTime), "20120106 10:14:15.000-0800");
  }

  public void testWasNull() throws Exception {
    CSVFileDataSource source = getTestExposureSource("exposure");
    source.next();
    source.next();  // go to the second row
    Date updateTime = source.getDate("updateTime");
    Assert.assertTrue(source.wasNull());
    Date createTime = source.getTimestamp("createTime");
    Assert.assertFalse(source.wasNull());
  }

  private CSVFileDataSource getTestExposureSource(String exposure) throws IOException {
    return new CSVFileDataSource(exposure, getTestExposures());
  }

  private CSVFileDataSource getEmptyExposureSource(String exposure) throws IOException {
    return new CSVFileDataSource(exposure, getEmptyExposures());
  }

  private InputStream getTestExposures() {
    return getClass().getClassLoader().getResourceAsStream("test_exposures.csv");
  }
  private InputStream getEmptyExposures() {
    return getClass().getClassLoader().getResourceAsStream("empty_exposures.csv");
  }
}
