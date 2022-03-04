package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.RowDataSource;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class description...
 *
 */
public abstract class ColumnDef {
  private String name;
  private String type;
  public static String INTEGER = "INTEGER";
  public static String STRING = "STRING";
  public static String SUBSTRING = "SUBSTRING";
  public static String DATE = "DATE";
  public static String DATETIME = "DATETIME";
  public static String DECIMAL = "DECIMAL";
  public static String BIT = "BIT";
  public static String TYPECODE = "TYPECODE";
  public static String TYPECODEID = "TYPECODEID";
  public static String ID = "ID";
  // The BRACKET replacements are used when $1 or $2 have parentheses in them.  Because of the difficulty
  // of handling internal parentheses in regexes, we use a brackets as the marker of the beginning and end
  // of the strings to be parsed.
  public static String INTEGER_BRACKET = "INTEGER_BRACKET";
  public static String STRING_BRACKET = "STRING_BRACKET";
  public static String SUBSTRING_BRACKET = "SUBSTRING_BRACKET";
  public static String DATE_BRACKET = "DATE_BRACKET";
  public static String DATETIME_BRACKET = "DATETIME_BRACKET";
  public static String DECIMAL_BRACKET = "DECIMAL_BRACKET";
  public static String BIT_BRACKET = "BIT_BRACKET";
  public static String TYPECODE_BRACKET = "TYPECODE_BRACKET";
  public static String TYPECODEID_BRACKET = "TYPECODEID_BRACKET";
  public static String ID_BRACKET = "ID_BRACKET";

  public static String NUMBER = "NUMBER";  // Oracle type -> DECIMAL
  public static String VARCHAR2 = "VARCHAR2";  // Oracle type -> DECIMAL
  public static String FROM_DATE = "FROM_DATE";  // Functions to get part of a date as a string will have this as a suffix


  public ColumnDef(String name, String type) {
    this.name = name;
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnDef)) {
      return false;
    }

    ColumnDef columnDef = (ColumnDef) o;

    if (!name.equals(columnDef.name) || !getType().equals(columnDef.getType())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return name.hashCode() + getType().hashCode();
  }

  public abstract String getOutputResults(ResultSet row) throws SQLException;
  public abstract String getOutputResults(RowDataSource row) throws SQLException, ParseException;

  public String getName() {
    return name;
  }

  public static ColumnDef createDefinition(String type, String name, String formatting) {
    if (BIT.equals(type) || BIT_BRACKET.equals(type)) {
      return new BitColumn(name, BIT);
    } else if (INTEGER.equals(type) || INTEGER_BRACKET.equals(type)) {
      return new IntegerColumn(name, INTEGER);
    } else if (ID.equals(type) || ID_BRACKET.equals(type)) {
      return new IntegerColumn(name, ID);
    } else if (TYPECODEID.equals(type) || TYPECODEID_BRACKET.equals(type)) {
      return new IntegerColumn(name, TYPECODEID);
    } else if (SUBSTRING.equals(type) || SUBSTRING_BRACKET.equals(type)) {
      Pattern pattern = Pattern.compile("\\s*(.+?)\\s*,\\s*(.+?)\\s*,\\s*(.+?)");
      Matcher matcher = pattern.matcher(name);
      if (!matcher.matches()) {
        throw new UnknownColumnTypeException("Cannot parse: " + name + " for substring");
      }
      return new StringColumn(matcher.group(3), STRING);
    } else if (STRING.equals(type) || STRING_BRACKET.equals(type)) {
      return new StringColumn(name, STRING);
    } else if (TYPECODE.equals(type) || TYPECODE_BRACKET.equals(type)) {
      return new StringColumn(name, TYPECODE);
    } else if (type != null && type.endsWith(FROM_DATE)) {
      return new StringColumn(name, STRING);
    } else if (DATETIME.equals(type) || DATETIME_BRACKET.equals(type)) {
      return new DateTimeColumn(name, DATETIME, formatting);
    } else if (DATE.equals(type)  || DATE_BRACKET.equals(type)) {
      return new DateColumn(name, DATE, formatting);
    } else if (DECIMAL.equals(type) || DECIMAL_BRACKET.equals(type)) {
      return new DecimalColumn(name, DECIMAL);
    }
    throw new UnknownColumnTypeException("Unknown type: " + type + "; valid types are INTEGER, STRING, DATETIME, and DECIMAL");
  }

  public static ColumnDef createDefinition(String type, String name) {
    return createDefinition(type, name, null);
  }

  public static ColumnDef createFromOracle(String type, String name) {
    try {
      return createDefinition(type, name);
    } catch (UnknownColumnTypeException ucte) {
    }
    if (NUMBER.equals(type)) {
      return new DecimalColumn(name, DECIMAL);
    }
    if (VARCHAR2.equals(type)) {
      return new StringColumn(name, STRING);
    }
    throw new UnknownColumnTypeException("Unknown type: " + type + "; valid types are INTEGER, STRING, DATETIME, and DECIMAL");
  }

  public String getType() {
    return type;
  }

  public boolean isUpdateTime() {
    return "updateTime".equals(name);
  }

  public abstract String getDBColumnType();

  public abstract void setArgument(int i, PreparedStatement sql, String s) throws SQLException;

  public Date getDate(String val) throws ParseException {
    throw new UnsupportedOperationException("Cannot get date from this column definition");
  }
  public String getString(String val) throws ParseException {
    return val;
  }
  public Integer getInt(String val) throws ParseException {
    throw new UnsupportedOperationException("Cannot get int from this column definition");
  }
  public Double getDouble(String val) throws ParseException {
    throw new UnsupportedOperationException("Cannot get double from this column definition");
  }
  public Timestamp getTimestamp(String s) throws ParseException {
    throw new UnsupportedOperationException("Cannot get timestamp from this column definition");
  }

  // Date and DateTime columnDefs take an optional format string. The getter needs to be here to correctly
  // serialize those column defs.
  public String getFormatString() {
    return null;
  }

  public boolean wasNull(String s) {
    return s == null || s.trim().length() == 0;
  }

  public abstract boolean isCompatible(ColumnDef expectedColumnDefinition);

  public boolean isStringCompatible() {
    return false;
  }
  public boolean isDateCompatible() {
    return false;
  }
  public boolean isDecimalCompatible() {
    return isIntegerCompatible();
  }
  public boolean isIntegerCompatible() {
    return false;
  }

  public static class UnknownColumnTypeException extends RuntimeException {
    public UnknownColumnTypeException(String s) {
      super(s);
    }
  }

  public static ColumnDefDeserializer createJSONDeserializer() {
    return new ColumnDefDeserializer();
  }

  public static class ColumnDefDeserializer implements JsonDeserializer<ColumnDef> {
    @Override
    public ColumnDef deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
      try {
        JsonObject jsonObject = (JsonObject)json;
        String type = jsonObject.get("type").getAsString();
        String name = jsonObject.get("name").getAsString();
        String formatting = null;
        if (jsonObject.get("formatString") != null) {
          formatting = ((JsonObject) json).get("formatString").getAsString();
        }
        return ColumnDef.createDefinition(type, name, formatting);
      } catch (Throwable t) {
        t.printStackTrace();
        return null;
      }
    }
  }
}
