package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.RowDataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;


public class BitColumn extends IntegerColumn {

  BitColumn(String name, String type) {
    super(name, type);
  }

  @Override
  public String getDBColumnType() {
    return "TINYINT";
  }


  @Override
  public String getOutputResults(ResultSet row) throws SQLException {
    try {
      return super.getOutputResults(row);
    }
    // In postgres, row.getInt() doesn't work so need to handle it as a boolean
    catch (Exception e) {
      return row.wasNull() ? "" : (row.getBoolean(getName()) ? "1" : "0");
    }
  }

  @Override
  public String getOutputResults(RowDataSource row) throws SQLException, ParseException {
    try {
      return super.getOutputResults(row);
    }
    catch (Exception e) {
      String result = row.getString(getName());
      if (row.wasNull() || result == null) {
        return "";
      }
      return Boolean.parseBoolean(result) ? "1" : "0";
    }
  }
}
