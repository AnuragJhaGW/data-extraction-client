package com.guidewire.tools.benchmarking;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Class description...
 */
@Test(groups="unit")
public class IntegerColumnTest {

  public void testLargeNumbersDoNotIncludeCommas() throws Exception {
    IntegerColumn column = (IntegerColumn)ColumnDef.createDefinition(ColumnDef.INTEGER, "test");
    
    Assert.assertEquals(column.getOutputResults(100000), "100000");
  }
}
