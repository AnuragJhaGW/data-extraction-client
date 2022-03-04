package com.guidewire.tools.benchmarking;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Class description...
 */
@Test(groups="unit")
public class ColumnDefTest {
  public void testCreateDefinition() throws Exception {
    ColumnDef nospaces = ColumnDef.createDefinition("SUBSTRING", "1,11,vin");
    ColumnDef allSpaces = ColumnDef.createDefinition("SUBSTRING", " 1 , 11 , vin");
    ColumnDef someSpacing = ColumnDef.createDefinition("SUBSTRING", "1, 11, vin");

    Assert.assertEquals(nospaces.getName(), "vin");
    Assert.assertEquals(allSpaces.getName(), "vin");
    Assert.assertEquals(someSpacing.getName(), "vin");
  }
}
