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
 *
 */
public class IntegerColumn extends ColumnDef {

  private transient NumberFormat formatter;

  public IntegerColumn(String name, String type) {
    super(name, type);
    formatter = NumberFormat.getNumberInstance(Locale.US);
    formatter.setGroupingUsed(false);
  }

  @Override
  public String getOutputResults(ResultSet row) throws SQLException {
    int result = row.getInt(getName());
    if (row.wasNull()) {
      return "";
    }
    return getOutputResults(result);
  }

  @Override
  public String getOutputResults(RowDataSource row) throws SQLException, ParseException {
    Integer result = row.getInt(getName());
    if (row.wasNull() || result == null) {
      return "";
    }
    return getOutputResults(result);
  }

  @Override
  public String getDBColumnType() {
    return "INT(11)";
  }

  String getOutputResults(int result) {
    return formatter.format(result);
  }

  @Override
  public Integer getInt(String s) throws ParseException {
    if (wasNull(s)) return null;

    return formatter.parse(s).intValue();
  }

  @Override
  public boolean isCompatible(ColumnDef expectedColumnDefinition) {
    return expectedColumnDefinition.isIntegerCompatible();
  }

  @Override
  public boolean isIntegerCompatible() {
    return true;
  }

  @Override
  public void setArgument(int i, PreparedStatement sql, String s) throws SQLException {
    try {
      Integer val = getInt(s);
      if (val == null) {
        sql.setNull(i, Types.INTEGER);
      } else {
        sql.setInt(i, val);
      }
    } catch (ParseException e) {
      sql.setInt(i, -999);
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
