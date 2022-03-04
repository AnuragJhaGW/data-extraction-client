package com.guidewire.tools.benchmarking;

/**
* Class description...
*/
public class CustomerConfig {
  public static final String ORACLE = "oracle";
  public static final String SQLSERVER = "sqlserver";

  private String code;
  private String DBMS;
  private String version;
  private String name;

  public void setCode(String value) {
    code = value;
  }
  public boolean isOracle() {
    return ORACLE.equals(DBMS);
  }
  public boolean isSQLServer() {
    return SQLSERVER.equals(DBMS);
  }
  public String getDB() {
    return DBMS;
  }
  public void setDB(String value) throws InvalidDatabaseType {
    if (DBMS == null ||
            DBMS.equals(ORACLE) ||
            DBMS.equals(SQLSERVER)) {
      DBMS = value;
    } else {
      throw new InvalidDatabaseType("Cannot configure for database: " + value);
    }
  }
  public void setName(String value) {
    name = value;
  }

  public void setVersion(String value) {
    version = value;
  }

  public String getCode() {
    return code;
  }

  public String getUploadUser() {
    return code + "upload";
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public class InvalidDatabaseType extends Exception {
    public InvalidDatabaseType(String s) {
      super(s);
    }
  }
}
