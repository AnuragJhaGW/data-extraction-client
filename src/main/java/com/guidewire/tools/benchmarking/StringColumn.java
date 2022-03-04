package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.RowDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class description...
 *
 * @author rvernick
 */
public class StringColumn extends ColumnDef {
  transient String backslashRepresentation = "<%gwrebkslsh%>";
  transient Pattern backslash = Pattern.compile(backslashRepresentation);
  transient Pattern trailingBackslash = Pattern.compile("\\\\");
  transient Pattern carriageReturn = Pattern.compile("\r");

  public StringColumn(String name, String type) {
    super(name, type);
  }

  @Override
  public String getOutputResults(ResultSet row) throws SQLException {
    String result = row.getString(getName());
    if (row.wasNull()) {
      return "";
    }
    return escapeSpecialCharacters(result);
  }

  @Override
  public String getOutputResults(RowDataSource row) throws SQLException, ParseException {
    String result = row.getString(getName());
    if (row.wasNull()) {
      return "";
    }
    return escapeSpecialCharacters(result);
  }

  @Override
  public String getDBColumnType() {
    if (TYPECODE.equals(getType())) {
      return "VARCHAR(50)";
    }
    return "VARCHAR(256)";
  }

  protected String escapeSpecialCharacters(String string) {
    Matcher backslashMatcher = trailingBackslash.matcher(string);
    String backslashEscaped = backslashMatcher.replaceAll(backslashRepresentation);
    Matcher cr = carriageReturn.matcher(backslashEscaped);
    return cr.replaceAll("\n");
  }

  protected String unescapeSpecialCharacters(String s) {
    Matcher escapedBackslashes = backslash.matcher(s);
    return escapedBackslashes.replaceAll("\\\\");
  }

  @Override
  public void setArgument(int i, PreparedStatement sql, String s) throws SQLException {
    if (wasNull(s)) {
      sql.setNull(i, Types.VARCHAR);
    } else {
      sql.setString(i, unescapeSpecialCharacters(s));
    }
  }

  @Override
  public boolean isCompatible(ColumnDef expectedColumnDefinition) {
    return expectedColumnDefinition.isStringCompatible();
  }

  @Override
  public boolean isStringCompatible() {
    return true;
  }
}