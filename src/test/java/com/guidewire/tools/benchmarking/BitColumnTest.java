package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.RowDataSource;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.List;


@Test(groups="unit")
public class BitColumnTest {

  public void testCanGetIntResults() {
    BitColumn column = (BitColumn) ColumnDef.createDefinition(ColumnDef.BIT, "test");
    Assert.assertEquals(column.getOutputResults(123), "123");
  }

  public void testCanGetBoolResults() throws SQLException, ParseException {
    BitColumn column = (BitColumn) ColumnDef.createDefinition(ColumnDef.BIT, "test");

    String outputResults = column.getOutputResults(rowDataSource(true));
    Assert.assertEquals(outputResults, "1");

    outputResults = column.getOutputResults(rowDataSource(false));
    Assert.assertEquals(outputResults, "0");
  }


  private RowDataSource rowDataSource(boolean value) {
    return new RowDataSource() {
      @Override
      public QueryDefinition getQueryDefinition() {
        return null;
      }

      @Override
      public String getName() {
        return null;
      }

      @Override
      public List<ColumnDef> getColumns() {
        return null;
      }

      @Override
      public boolean next() {
        return true;
      }

      @Override
      public void close() {

      }

      @Override
      public Date getDate(String date) {
        return null;
      }

      @Override
      public Timestamp getTimestamp(String timestamp) {
        return null;
      }

      @Override
      public Double getDouble(String value) {
        return null;
      }

      @Override
      public Integer getInt(String integer) throws ParseException {
        throw new ParseException("ParseException", 0);
      }

      @Override
      public String getString(String s) {
        return value ? "true" : "false";
      }

      @Override
      public boolean wasNull() {
        return false;
      }

      @Override
      public void setTimeToRun(long timeToRun) {

      }

      @Override
      public long getTimeToRun() {
        return 0;
      }

      @Override
      public boolean expectedRowsKnown() {
        return false;
      }

      @Override
      public Long getExpectedRows() {
        return null;
      }

      @Override
      public long getCurrentRowNumber() {
        return 0;
      }

      @Override
      public void writeToBadFile(DataExtractor extractor) {

      }

      @Override
      public boolean rowSuccessfullyRead() {
        return false;
      }
    };
  }

}

