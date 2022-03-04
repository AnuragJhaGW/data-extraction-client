package com.guidewire.tools.benchmarking;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class description...
 */
public class DataExtractSQLFormat {

  private static final String TYPECODE_TABLE_NAME = "cbs_stg_typecode";
  public static final String TYPECODE_TYPE = "type";
  private static final String ODS_TYPECODE_TABLE_NAME = "ods_typecode";
  private static final String DATE_COUNT = "date_count_";
  private static final int DATE_COUNT_LENGTH = DATE_COUNT.length();

  public String getTableName(String name) {
    if (isTypecode(name)) {
      return TYPECODE_TABLE_NAME;
    }
    return "cbs_stg_" + name;
  }

  public boolean isTypecode(String name) {
    Pattern pattern = Pattern.compile("cctl_");
    Matcher matcher = pattern.matcher(name);
    return matcher.find();
  }
  
  public String typecodeType(String name) {
    return name.substring(5);
  }

  public String getODSTableName(String name) {
    if (isTypecode(name)) {
      return ODS_TYPECODE_TABLE_NAME;
    }
    return "ods_" + name;
  }

  public boolean isDateCount(String name) {
    if (name == null || "".equals(name))
      return false;
    else
      return name.startsWith(DATE_COUNT);
  }

  public String dateCountTable(String name) {
    return name.substring(DATE_COUNT_LENGTH);
  }
}
