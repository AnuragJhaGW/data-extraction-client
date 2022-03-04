package com.guidewire.tools.benchmarking;

import junit.framework.Assert;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jjohnson
 * Date: 3/30/15
 * Time: 3:18 PM
 * To change this template use File | Settings | File Templates.
 */
@Test(groups="unit")
public class DateTimeColumnTest {

  public void testDefaultDateTimeFormats() {
    DateTimeColumn column = (DateTimeColumn)ColumnDef.createDefinition(ColumnDef.DATETIME, "test");
    List<SimpleDateFormat> dateFormats = column.getDateFormats();
    validateDateTimeFormats(column, dateFormats);
  }

  private String[] formatStrings =  {"yyyyMMdd HH:mm:ss.sssz",
          "EEE, d MMM yyyy hh 'o''clock' a, z", "MMMM d, yyyy HH:mm:ss",
          "yyyyMMdd HHmmss",  "dd.MM.yyyy HH:mm:ss",
          "yyyy MMM dd hh:mm:ss a"
  };

  public void testCustomDateTimeFormats() {
    Date date = new Date();
    for (String formatString : formatStrings) {
      SimpleDateFormat format = new SimpleDateFormat(formatString);
      String dateInFormat = format.format(date);
      DateTimeColumn column = (DateTimeColumn)ColumnDef.createDefinition(ColumnDef.DATETIME, "test", formatString);
      validateDateTimeFormat(column, dateInFormat, format);
    }
  }

  private void validateDateTimeFormats(DateTimeColumn column, List<SimpleDateFormat> dateFormats) {
    Date date = new Date();
    for (SimpleDateFormat format : dateFormats) {
      String dateInFormat = format.format(date);
      validateDateTimeFormat(column, dateInFormat, format);
    }
  }

  private void validateDateTimeFormat(DateTimeColumn column, String dateInFormat, SimpleDateFormat format) {
    try {
      column.dateFor(dateInFormat);
    } catch (ParseException pe) {
      Assert.fail("Got unparseable date exception for format " + format.toPattern() + "  " + pe.getMessage());
    }
  }
}
