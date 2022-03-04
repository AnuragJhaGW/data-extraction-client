package com.guidewire.tools.benchmarking;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.locks.Condition;

/**
 * Class description...
 */
@Test(groups="unit")
public class DecimalColumnTest {

  public void testLargeNumbersDoNotIncludeCommas() throws Exception {
    DecimalColumn column = (DecimalColumn)ColumnDef.createDefinition(ColumnDef.DECIMAL, "test");
    
    Assert.assertEquals(column.getOutputResults(100000), "100000");
  }

  public void testLargeNumbersUsesPeriods() throws Exception {
    DecimalColumn column = (DecimalColumn)ColumnDef.createDefinition(ColumnDef.DECIMAL, "test");

    Assert.assertEquals(column.getOutputResults(100000.2), "100000.2");
    Assert.assertEquals(column.getOutputResults(100000.23), "100000.23");
    Assert.assertEquals(column.getOutputResults(100000.234), "100000.234");
    Assert.assertEquals(column.getOutputResults(10000.02345), "10000.02345");
    Assert.assertEquals(column.getOutputResults(10000.01234567890123), "10000.0123456789");
    Assert.assertEquals(column.getOutputResults(0.2345), "0.2345");
    Assert.assertEquals(column.getOutputResults(0.02345), "0.02345");
    Assert.assertEquals(column.getOutputResults(0.01234567890123), "0.0123456789");
  }

}
