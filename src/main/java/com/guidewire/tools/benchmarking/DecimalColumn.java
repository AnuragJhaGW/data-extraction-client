package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.RowDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

/**
 * Class description...
 */
public class DecimalColumn extends ColumnDef {
  private transient NumberFormat formatter;

  public DecimalColumn(String name, String type) {
    super(name, type);
    formatter = NumberFormat.getNumberInstance(Locale.US);
    formatter.setGroupingUsed(false);
    formatter.setMaximumFractionDigits(10);
  }

  @Override
  public String getOutputResults(ResultSet row) throws SQLException {
    Double value = row.getDouble(getName());
    if (row.wasNull() || value == null) {
      return "";
    }
    return getOutputResults(value);
  }

  @Override
  public String getOutputResults(RowDataSource row) throws SQLException, ParseException {
    Double value = row.getDouble(getName());
    if (row.wasNull() || value == null) {
      return "";
    }
    return getOutputResults(value);
  }

  @Override
  public String getDBColumnType() {
    return "DECIMAL(18,2)";
  }

  public String getOutputResults(double value) throws SQLException {
    return formatter.format(value);
  }

  @Override
  public Double getDouble(String s) throws ParseException {
    if (wasNull(s)) {
      return null;
    }
    return formatter.parse(s).doubleValue();
  }

  @Override
  public boolean isCompatible(ColumnDef expectedColumnDefinition) {
    return expectedColumnDefinition.isDecimalCompatible();
  }

  @Override
  public boolean isDecimalCompatible() {
    return true;
  }

  @Override
  public void setArgument(int i, PreparedStatement sql, String s) throws SQLException {
    try {
      Double val = getDouble(s);
      if (val == null) {
        sql.setNull(i, Types.DECIMAL);
      } else {
        sql.setDouble(i, val);
      }
    } catch (ParseException e) {
      sql.setInt(i, -999);
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
