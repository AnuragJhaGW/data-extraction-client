package com.guidewire.tools.benchmarking;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.guidewire.cloudviewer.datamoving.RowDataSource;

import java.lang.reflect.Type;
import java.security.InvalidParameterException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;

/**
 * The definition of a column for a customer csv file definition. This column definition delegates to the query column definition
 * for name and type handling, but it also has a required attribute and a possible alias. The alias allows customers to map their
 * column names to the column that we expect to have.
 */
public class CSVFileColumnDef extends ColumnDef {
  private final transient String stringReplacementRegex = "[_\\s]";
  protected ColumnStatus columnStatus; // Is the column required in all csv  files?
  protected String alias; // The set of names that we will accept in the header line of the file as being the name of this column
  protected ColumnDef actualColumnDef;  // the column def that stores the name, data type and optional format string

  // The column can be required, not required but allowed, or omitted. If omitted, the present of this column in the data file will
  // generate a validation error
  public enum ColumnStatus {
    REQUIRED,
    NOTREQUIRED,
    OMITTED
  }

  public CSVFileColumnDef(String name, String type, String alias, ColumnStatus columnStatus) {
    super(name, type);
    actualColumnDef = ColumnDef.createDefinition(type, name);
    this.columnStatus = columnStatus;
    setAlias(alias);
  }

  public CSVFileColumnDef(String name, String type, String dateFormat, String alias, ColumnStatus columnStatus) {
    super(name, type);
    setAlias(alias);
    this.columnStatus = columnStatus;
    actualColumnDef = ColumnDef.createDefinition(type, name, dateFormat);
  }

  @Override
  public String getOutputResults(ResultSet row) throws SQLException {
    return actualColumnDef.getOutputResults(row);
  }

  @Override
  public String getOutputResults(RowDataSource row) throws SQLException, ParseException {
    return actualColumnDef.getOutputResults(row);
  }

  @Override
  public String getDBColumnType() {
    return actualColumnDef.getDBColumnType();
  }

  @Override
  public void setArgument(int i, PreparedStatement sql, String s) throws SQLException {
    // Do nothing here?? This appears to only be relevant to the query column def stuff
  }

  @Override
  public boolean isCompatible(ColumnDef expectedColumnDefinition) {
    return actualColumnDef.isCompatible(expectedColumnDefinition);
  }

  public ColumnStatus getColumnStatus() {
    return columnStatus;
  }

  public String getAlias() {
    return alias;
  }

  private void setAlias(String value) {
    alias = value.replaceAll(stringReplacementRegex, "");
  }

  @Override
  public String getName() {
    return actualColumnDef.getName();
  }

  @Override
  public String getType() {
    return actualColumnDef.getType();
  }

  @SuppressWarnings("UnusedDeclaration")
  public ColumnDef getActualColumnDef() {
    return actualColumnDef;
  }

  @Override
  public Date getDate(String val) throws ParseException {
    val = trimAndValidateRequiredFieldValue(val);
    return actualColumnDef.getDate(val);
  }

  @Override
  public String getString(String val) throws ParseException{
    val = trimAndValidateRequiredFieldValue(val);
    return actualColumnDef.getString(val);
  }

  public String getStringWithoutValidate(String val) throws ParseException{
    return actualColumnDef.getString(val);
  }

  @Override
  public Integer getInt(String val) throws ParseException {
    val = trimAndValidateRequiredFieldValue(val);
    return actualColumnDef.getInt(val);
  }

  @Override
  public Double getDouble(String val) throws ParseException {
    val = trimAndValidateRequiredFieldValue(val);
    return actualColumnDef.getDouble(val);
  }

  @Override
  public Timestamp getTimestamp(String val) throws ParseException {
    val = trimAndValidateRequiredFieldValue(val);
    return actualColumnDef.getTimestamp(val);
  }

  @Override
  public String getFormatString() {
    return actualColumnDef.getFormatString();
  }

  public boolean isRequired() {
    return getColumnStatus().equals(ColumnStatus.REQUIRED);
  }

  public boolean isOmitted() {
    return getColumnStatus().equals(ColumnStatus.OMITTED);
  }

  @SuppressWarnings("UnusedDeclaration")
  public boolean isNotRequired() {
    return getColumnStatus().equals(ColumnStatus.NOTREQUIRED);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CSVFileColumnDef)) {
      return false;
    }
    CSVFileColumnDef columnDef = (CSVFileColumnDef) o;
    if (!getName().equals(columnDef.getName())) {
      return false;
    } else if (!getType().equals(columnDef.getType())) {
      return false;
    } else if (!getAlias().equals(columnDef.getAlias())) {
      return false;
    }  else if (actualColumnDef.getFormatString() != null && !actualColumnDef.getFormatString().equals(columnDef.getFormatString())) {
      return false;
    }
    return getColumnStatus() == columnDef.getColumnStatus();
  }

  public static CSVColumnDefDeserializer createCSVJSONDeserializer() {
    return new CSVColumnDefDeserializer();
  }

  /**
   * Returns true iff the name passed in matches one of the alias.  We match case insensitive and we eliminate all
   * whitespace in the passed in name
   *
   * @param nameToTest String to test against the alias of this column
   * @return true iff the name matches an alias
   */
  public boolean nameMatches(String nameToTest) {
    if (nameToTest == null) {
      return false;
    }

    // Remove whitespace and _ characters
    String toTest = nameToTest.replaceAll(stringReplacementRegex, "");
    String columnDefNameToTest = actualColumnDef.getName().replaceAll(stringReplacementRegex, "");
    return columnDefNameToTest.equalsIgnoreCase(toTest);
  }

  public boolean aliasMatches(String nameToTest) {
    if (nameToTest == null) {
      return false;
    }

    // Remove whitespace and _ characters
    String toTest = nameToTest.replaceAll(stringReplacementRegex, "");
    return alias.equalsIgnoreCase(toTest);
  }

  protected void applyTo(CSVFileColumnDef baseColumnDef) {
    baseColumnDef.setAlias(getAlias());
    if (hasFormattingOption()){
      baseColumnDef.setActualColumnDef(getActualColumnDef());
    }
    if (baseColumnDef.isRequired() && !getColumnStatus().equals(ColumnStatus.REQUIRED)) {
      throw new InvalidParameterException("A required column cannot be changed to not required or omitted status");
    }
    baseColumnDef.setColumnStatus(getColumnStatus());
  }

  private String trimAndValidateRequiredFieldValue(String val) throws ParseException {
    if (val != null) {
      val = val.trim();
    }
    if (isRequired() && (val == null || val.equals(""))) {
      throw new ParseException("Null value found for required field " + getName() + " of type " + getType(), 0);
    }
    return val;
  }

  private void setColumnStatus(ColumnStatus status) {
    this.columnStatus = status;
  }

  private boolean hasFormattingOption() {
    return actualColumnDef instanceof DateColumn || actualColumnDef instanceof DateTimeColumn;
  }
  private void setActualColumnDef(ColumnDef actualColumnDef) {
    this.actualColumnDef = actualColumnDef;
  }
  public static class CSVColumnDefDeserializer implements JsonDeserializer<ColumnDef> {

    @Override
    public CSVFileColumnDef deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
      try {
        JsonObject jsonObject = (JsonObject)json;
        String columnstatus = jsonObject.get("columnStatus").getAsString();
        ColumnStatus status = columnstatus.equals("REQUIRED") ? ColumnStatus.REQUIRED : columnstatus.equals("OMITTED") ? ColumnStatus.OMITTED : ColumnStatus.NOTREQUIRED;
        String alias = null;
        if (jsonObject.get("alias") != null) {
          alias = ((JsonObject) json).get("alias").getAsString();
        }
        JsonObject actualColumnDefJsonObject = jsonObject.get("actualColumnDef").getAsJsonObject();
        String formatting = null;
        if (actualColumnDefJsonObject.get("formatString") != null) {
          formatting = actualColumnDefJsonObject.get("formatString").getAsString();
        }
        String type = actualColumnDefJsonObject.get("type").getAsString();
        String name = actualColumnDefJsonObject.get("name").getAsString();
        return new CSVFileColumnDef(name, type, formatting, alias, status);
      } catch (Throwable t) {
        t.printStackTrace();
        return null;
      }
    }

  }
}
