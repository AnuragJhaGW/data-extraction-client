package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.RowDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class description...
 *
 * @author rvernick
 */
public class DateColumn extends ColumnDef {
  protected static final String YYYYMMDD = "yyyyMMdd";
  String formatString = YYYYMMDD;
  transient SimpleDateFormat format = new SimpleDateFormat(YYYYMMDD);

  public DateColumn(String name, String type, String simpleDateFormatString) {
    super(name, type);
    if (simpleDateFormatString != null && !"".equals(simpleDateFormatString)) {
      formatString = simpleDateFormatString;
      format = new SimpleDateFormat(formatString);
    }
  }

  @Override
  public String getOutputResults(ResultSet row) throws SQLException {
    Date columnData = row.getDate(getName());
    if (row.wasNull()) {
      return "";
    }
    return format.format(columnData);
  }

  @Override
  public String getOutputResults(RowDataSource row) throws SQLException, ParseException {
    Date columnData = row.getDate(getName());
    if (row.wasNull()) {
      return "";
    }
    return format.format(columnData);
  }

  @Override
  public String getDBColumnType() {
    return "DATE";
  }

  @Override
  public Date getDate(String s) throws ParseException {
    if (wasNull(s)) return null;

    return format.parse(s);
  }

  @Override
  public String getFormatString() {
    return formatString;
  }

  @Override
  public void setArgument(int i, PreparedStatement sql, String s) throws SQLException {
    if (s.length() > 0) {
      try {
        Date date = format.parse(s);
        sql.setDate(i, new java.sql.Date(date.getTime()));
      } catch (ParseException e) {
        e.printStackTrace();
      }
    } else {
      sql.setNull(i, Types.DATE);

    }
  }

  @Override
  public boolean isCompatible(ColumnDef expectedColumnDefinition) {
    return expectedColumnDefinition.isDateCompatible();
  }

  public boolean isDateCompatible() {
    return true;
  }
}
