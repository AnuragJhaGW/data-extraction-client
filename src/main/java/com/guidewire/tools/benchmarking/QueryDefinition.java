package com.guidewire.tools.benchmarking;

import com.guidewire.tools.MiscUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A QueryDefinition contains a variety of information used to efficiently pull data from a database.  In particular,
 * it contains:
 *    name - an indicator of what the query is getting (also used to find the appropriate staging table)
 *    transformedSQL - the query itself
 *    columns - A collection of Column Definitions matching the expected output columns from the query
 *    countSQL - a query to get a count of how many rows it expects
 *    checksumSQL - A query to get a value uniquely calculated on the entire dataset
 *    latestUpdateDate - an indicator of when the most recent data is, allowing us to incrementally query beyond it
 *    lastChecksum - a calculation on the whole dataset allowing us to check if the data has changed at all useful for data which doesn't have updateTime information
 *    isCatchUp - a flag to indicate if this query needs to run on the entire data set during an incremental update
 *    versionsToExclude - a collection of strings representing the versions of the application for which this query is not valid (table does not exist)
 *
 * There are a few other operational fields.  For chuncking the query into different date ranges, there are several fields:
 *    chunk - whether the query can be chunked at all
 *    earliestDate - the date to start the chunking of a full query
 *    daysForEachChunk - a means of controlling the size of each chunk
 */
public class QueryDefinition {

  private static final int DEFAULT_DAYS_PER_QUERY_CHUNK = 30;

  private String name;
  String originalSQL;
  private String transformedSQL;
  List<ColumnDef> columns = new ArrayList<>();
  private String countSQL = null;
  private String checksumSQL = "";
  private Date latestUpdateDate = null;
  private Long lastChecksum;
  private boolean isCatchUp = false;
  private Set<Integer> versionsToExclude = new HashSet<>();
  private boolean chunk = false;
  private Date earliestDate = getDefaultEarliestDate();
  private int daysForEachChunk = DEFAULT_DAYS_PER_QUERY_CHUNK;
  private String version = "1.0";
  private boolean incremental = false;
  private boolean lakeOnly = true;


  /**
   * Creates a query definition specific to typecode tables
   * @param typecodeTable name of the typecode table to be queried
   */
  public static QueryDefinition createBaseTypecodeQuery(String typecodeTable, String excludeList) {
    if (excludeList == null)
      excludeList = "";

    QueryDefinition result = new QueryDefinition();
    result.setName(typecodeTable);
    result.setCountSQL("&USEDB_SQL SELECT COUNT(*) FROM &DB_TAG" + typecodeTable);
    StringBuilder originalSQL = new StringBuilder();
    originalSQL.append("&USEDB_SQL\n");
    originalSQL.append("  &SELECT\n");
    originalSQL.append("    &TYPECODEID(tl.ID, typecodeid),\n");
    originalSQL.append("    &TYPECODE(tl.TYPECODE, typecode),\n");
    if (excludeList.contains("name")) {
      String excludeName = "ERASE_" + typecodeTable.toUpperCase() + "_NAME";
      originalSQL.append("    &" + excludeName + "(&STRING(tl.name, name),)\n");
    } else {
      originalSQL.append("    &STRING(tl.name, name),\n");
    }
    if (excludeList.contains("description")) {
      String excludeName = "ERASE_" + typecodeTable.toUpperCase() + "_DESCRIPTION";
      originalSQL.append("    &" + excludeName + "(&STRING(tl.description, description),)\n");
    } else {
      originalSQL.append("    &STRING(tl.description, description),\n");
    }
    if (excludeList.contains("retired")) {
      String excludeName = "ERASE_" + typecodeTable.toUpperCase() + "_RETIRED";
      originalSQL.append("    &" + excludeName + "(&BIT(tl.retired, retired),)\n");
    } else {
      originalSQL.append("    &BIT(tl.retired, retired),\n");
    }
    if (excludeList.contains("priority")) {
      String excludeName = "ERASE_" + typecodeTable.toUpperCase() + "_PRIORITY";
      originalSQL.append("    &" + excludeName + "(&INTEGER(tl.priority, priority),)\n");
    } else {
      originalSQL.append("    &INTEGER(tl.priority, priority)\n");
    }
    originalSQL.append("  &FROM\n");
    originalSQL.append("    &DB_TAG" + typecodeTable + " tl");
    result.setOriginalSQL(originalSQL.toString());
    result.setChecksumSQL("&CHECKSUM_AGG(" + typecodeTable + ")");
    return result;
  }

  public String getChecksumSQL() {
    return checksumSQL;
  }

  public void setChecksumSQL(String sql) {
    checksumSQL = sql;
  }

  public static Date getDefaultEarliestDate() {
    Calendar calendar = Calendar.getInstance();
    calendar.set(2000, Calendar.JANUARY, 1, 0, 0, 0);
    return calendar.getTime();
  }

  public Date getEarliestDate() {
    return earliestDate;
  }

  public void setEarliestDate(Date date) {
    earliestDate = date;
  }

  public String getOriginalSQL() {
    return originalSQL;
  }
  public void setOriginalSQL(String sql) {
    this.originalSQL = sql;
    transformedSQL = sql;
  }

  public boolean isIncremental() {
    return incremental;
  }
  public void setIncremental(boolean val) {
    incremental = val;
  }

  public boolean isLakeOnly() {
    return lakeOnly;
  }

  public void setLakeOnly(boolean lakeOnly) {
    this.lakeOnly = lakeOnly;
  }

  public Date getLatestUpdateDate() {
    return latestUpdateDate;
  }

  @SuppressWarnings("unused")
  public void setLatestUpdateDate(Date date) {
    latestUpdateDate = date;
  }


  /**
   * Substitutes values from the supplied properties into the query.  The substitution
   * is performed against the current transformedSQL and results in a new transformedSQL.
   * This allows multiple substitution passes to be performed.
   * <p/>
   * The properties are matched using Java regex.  For each key value pair,
   * all occurrences of the key text are replaced with the value text.  Capture
   * groups in the key can be referenced in the value.  For example, the original text:
   * <p/>
   * "&SELECT &DATE(clm.lossdate), &DATE(clm.reportdate) FROM cc_claim clm&EMPTY"
   * <p/>
   * and properties:
   * <p/>
   * &SELECT=SELECT
   * &DATE\\((.+?)\\)=CONVERT(VARCHAR(23), $1, 120)
   * &EMPTY=
   * <p/>
   * yields:
   * <p/>
   * "SELECT CONVERT(VARCHAR(23), clm.lossdate, 120), CONVERT(VARCHAR(23), clm.reportdate, 120) FROM cc_claim clm");
   * <p/>
   * Note the use of a reluctant qualifier ".+?" to stop the match at the first closing paren.
   * See http://docs.oracle.com/javase/tutorial/essential/regex/quant.html for more details
   *
   */
  public void transform(Properties properties) {
    if (transformedSQL != null) {
      transformedSQL = MiscUtils.transform(transformedSQL, properties);
    }
    if (countSQL !=  null) {
      countSQL = MiscUtils.transform(countSQL, properties);
    }
    if (checksumSQL != null) {
      checksumSQL = MiscUtils.transform(checksumSQL, properties);
    }
  }

  public PreparedStatement getTransformedQuery(Connection connection, String dbTag, String db) throws SQLException {
    return createPreparedStatement(connection, getTransformedSQL(dbTag, db));
  }

  public String getTransformedSQL(String dbTag, String db) {
    return MiscUtils.transform(transformedSQL, getDBTagAndNameProperties(dbTag, db));
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
  public List<ColumnDef> getColumns() {
    return columns;
  }

  public void addColumn(ColumnDef column) {
    columns.add(column);
  }

  public PreparedStatement getCountQuery(Connection connection, Date since, String dbTag, String db) throws SQLException {
    PreparedStatement result = createPreparedStatement(connection, getCountSQL(since, dbTag, db));
    if (isIncremental() && since != null) {
      result.setDate(1, new java.sql.Date(since.getTime()));
    }
    return result;
  }

  public String getCountSQL() {
    return countSQL;
  }

  public String getCountSQL(Date since, String dbTag, String db) {
    Properties properties = getDBTagAndNameProperties(dbTag, db);
    if (isIncremental() && since != null) {
      properties.setProperty("&SINCE_SQL\\((.+\\)?)\\s*?\\)", "$1");
    } else {
      properties.setProperty("&SINCE_SQL\\((.+\\)?)\\s*?\\)", "");
    }
    return MiscUtils.transform(countSQL, properties);
  }

  public String getChecksumSQL(String dbTag, String db) {
    return MiscUtils.transform(checksumSQL, getDBTagAndNameProperties(dbTag, db));
  }

  private Properties getDBTagAndNameProperties(String dbTag, String db) {
    Properties properties = new Properties();
    properties.setProperty("&DB_TAG", dbTag);
    properties.setProperty("&DB_NAME", db);
    return properties;
  }

  public void setCountSQL(String sql) {
    countSQL = sql;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String value) {
    version = value;
  }

  public void setDaysPerChunk(String chunk_size) {
    daysForEachChunk = new Integer(chunk_size);
  }

  public int getDaysForEachChunk() {
    return daysForEachChunk;
  }

  public boolean isChunk() {
    return chunk && daysForEachChunk > 0;
  }

  public void setChunk(boolean shouldChunk) {
    chunk = shouldChunk;
  }

  public Long getLastChecksum() {
    return lastChecksum;
  }

  @SuppressWarnings("unused")
  public void setLastChecksum(Long val) {
    lastChecksum = val;
  }

  public boolean isCatchUp() {
    return isCatchUp;
  }

  public void setCatchUp(boolean catchUp) {
    isCatchUp = catchUp;
  }

/*
  public String getCatchUpEntity() {
    return catchUpEntity;
  }

  public void setCatchUpEntity(String catchUpEntity) {
    this.catchUpEntity = catchUpEntity;
  }
*/

  public void buildColumnDefs() {
    List<ColumnDef> newDefs = getColumnDefs();
    if (!newDefs.isEmpty()) {
      columns = newDefs;
    }
  }

  /**
   * getColumnDefs() reads the transformed SQL and derives the column definitions based on the type and name of
   * each item in the select clause.
   * This method is not designed to be run twice.  Unless you know you need to generate these, you probably want
   * to call getColumns() instead.  It simply returns the columns that have been derived already.
   * @return List
   */
  protected List<ColumnDef> getColumnDefs() {
    List<ColumnDef> result = new ArrayList<>();
    String select = getSelectClause();

    String columnRegex = "\\s*&(\\w+)[\\(\\[](.+?)\\s*,\\s*(.+?)[\\)\\]]\\s*,*";
    Pattern pattern = Pattern.compile(columnRegex);
    final Matcher matcher = pattern.matcher(select);
    while (matcher.find()) {
      String type = matcher.group(1);
      String columnAlias = matcher.group(3);
      result.add(ColumnDef.createDefinition(type, columnAlias));
    }
    return result;
  }

  private String getSelectClause() {
    String selectRegex = "\\s*&SELECT(.+?)&FROM";
    Pattern pattern = Pattern.compile(selectRegex);
    final Matcher matcher = pattern.matcher(transformedSQL);
    if (matcher.find()) {
      String result = matcher.group(1);
      System.out.println(result);
      return result;
    } else {
      String nextRow = transformedSQL;
      int startIndex;
      int endIndex;
      String startRegex = "\\s*&SELECT(.*)";
      String middleRegex = "(.+)\n";
      String fromRegex = "(.*)&FROM.*";
      Pattern start = Pattern.compile(startRegex);
      Pattern middle = Pattern.compile(middleRegex);
      Pattern from = Pattern.compile(fromRegex);
      Matcher startMatch = start.matcher(transformedSQL);
      if (!startMatch.find()) {
        throw new MalformedSelectException("Query did not contain a select clause bounded by &SELECT and &FROM: " + transformedSQL);
      }
      startIndex = startMatch.start();
      endIndex = startMatch.end();
      nextRow = nextRow.substring(startMatch.end());
      Matcher fromMatch = from.matcher(nextRow);
      while (!fromMatch.find()) {
        Matcher middleMatch = middle.matcher(nextRow);
        if (!middleMatch.find()) {
          throw new MalformedSelectException("Query did not contain a select clause bounded by &SELECT and &FROM: " + transformedSQL);
        }
        endIndex += middleMatch.end();
        nextRow = nextRow.substring(middleMatch.end());
        fromMatch = from.matcher(nextRow);
      }
      endIndex += fromMatch.start();

      return transformedSQL.substring(startIndex, endIndex);
    }
  }

  @SuppressWarnings("unused")
  public void setEarliestDate(String earliest_date) throws ParseException {
    DateFormat translator = DateFormat.getDateInstance(DateFormat.SHORT, Locale.US);
    setEarliestDate(translator.parse(earliest_date));
  }

  // todo should switch the chunking to use binding variables, making it easier to deal with dates
  public String getChunkedSQL(Date earlier, Date later, String dbTag, String db) {
    SimpleDateFormat format = getDateFormatter();

    Properties substitution = new Properties();
    substitution.setProperty("&SINCE_SQL\\((.+\\)?)\\s*?\\)", "");
    substitution.setProperty("&SORT_BY_COLUMN", "createtime");
    substitution.setProperty("&CHUNK_SQL_NULL\\((.+)\\s*?\\)", "");
    if (earlier == null) {
      substitution.setProperty("&CHUNK_SQL_START\\((.+)\\s*?\\)", "");
    } else {
      substitution.setProperty("&CHUNK_SQL_START\\((.+)\\s*?\\)", "$1");
      substitution.setProperty("&CHUNK_START", "'" + format.format(earlier) + "'");
    }
    if (later == null) {
      substitution.setProperty("&CHUNK_SQL_END\\((.+)\\s*?\\)", "");
    } else {
      substitution.setProperty("&CHUNK_SQL_END\\((.+)\\s*?\\)", "$1");
      substitution.setProperty("&CHUNK_END", "'" + format.format(later) + "'");
    }
    String sql = new String(getTransformedSQL(dbTag, db));
    return MiscUtils.transform(sql, substitution);
  }

  public PreparedStatement getChunkedStatement(Connection dbConnection, Date earlier, Date later, String dbTag, String db) throws SQLException {
    String sql = getChunkedSQL(earlier, later, dbTag, db);
    PreparedStatement statement = dbConnection.prepareStatement(sql);
    setChunkedDates(statement, earlier, later);
    return statement;
  }

  public void setChunkedDates(PreparedStatement statement, Date earlier, Date later) throws SQLException {
    int arg = 1;
    if (earlier != null) {
      statement.setDate(arg++, new java.sql.Date(earlier.getTime()));
    }
    if (later != null) {
      statement.setDate(arg, new java.sql.Date(later.getTime()));
    }
  }

  private SimpleDateFormat getDateFormatter() {
    return new SimpleDateFormat("MM/dd/yyyy", Locale.US);
  }

  public PreparedStatement getNullChunkedQuery(Connection connection, String dbTag, String db) throws SQLException {
    return createPreparedStatement(connection, getNullChunkedSQL(dbTag, db));
  }

  public String getNullChunkedSQL(String dbTag, String db) {
    Properties substitution = new Properties();
    substitution.setProperty("&CHUNK_SQL_START\\((.+)\\s*?\\)", "");
    substitution.setProperty("&CHUNK_SQL_END\\((.+)\\s*?\\)", "");
    substitution.setProperty("&CHUNK_SQL_NULL\\((.+)\\s*?\\)", "$1");
    substitution.setProperty("&SINCE_SQL\\((.+\\)?)\\s*?\\)", "");
    substitution.setProperty("&SORT_BY_COLUMN", "updatetime");
    String sql = new String(getTransformedSQL(dbTag, db));
    return MiscUtils.transform(sql, substitution);
  }

  public void excludeCCVersion(Integer ccVersion) {
    versionsToExclude.add(ccVersion);
  }

  public void setVersionsToExclude(Set<Integer> versionsToExclude) {
    this.versionsToExclude = versionsToExclude;
  }

  public boolean isExcludedFor(Integer ccVersion) {
    return versionsToExclude.contains(ccVersion);
  }

  public boolean hasExclusions() {
    return !versionsToExclude.isEmpty();
  }

  public Set<Integer> getVersionsToExclude() {
    return versionsToExclude;
  }

  public PreparedStatement getIncrementalStatement(Connection connection, Date since, String dbTag, String db) throws SQLException {
    PreparedStatement statement = createPreparedStatement(connection, getIncrementalSQL(dbTag, db));
    statement.setTimestamp(1, new Timestamp(since.getTime()));
    return statement;
  }

  public PreparedStatement getChecksumStatement(Connection connection, String dbTag, String db) throws SQLException {
    return createPreparedStatement(connection, getChecksumSQL(dbTag, db));
  }

  /**
   * This method actually removes the chunking code, leaving the code which queries since a
   * particular date.
   * In order to support queries which are limited by update or create times, we've
   * built tags into the queries which help in 'CHUNKING' the table.  We need to be able
   * to CHUNK open ended at either end of time as well as dividing the live time.
   */
  public String getIncrementalSQL(String dbTag, String db) {
    Properties substitution = new Properties();
    substitution.setProperty("&CHUNK_SQL_START\\((.+)\\s*?\\)", "");
    substitution.setProperty("&CHUNK_SQL_END\\((.+)\\s*?\\)", "");
    substitution.setProperty("&CHUNK_SQL_NULL\\((.+)\\s*?\\)", "");
    substitution.setProperty("&SINCE_SQL\\((.+\\)?)\\s*?\\)", "$1");
    substitution.setProperty("&SORT_BY_COLUMN", "updatetime");
    String sql = new String(getTransformedSQL(dbTag, db));
    return MiscUtils.transform(sql, substitution);
  }

  private PreparedStatement createPreparedStatement(Connection connection, String sql) throws SQLException {
    Properties substitution = new Properties();
    substitution.setProperty("&CHUNK_SQL_START\\((.+)\\s*?\\)", "");
    substitution.setProperty("&CHUNK_SQL_END\\((.+)\\s*?\\)", "");
    substitution.setProperty("&SINCE_SQL\\((.+\\)?)\\s*?\\)", "");
    substitution.setProperty("&CHUNK_SQL_NULL\\((.+)\\s*?\\)", "");
    return connection.prepareStatement(MiscUtils.transform(sql, substitution));
  }

  public static class MalformedSelectException extends RuntimeException {
    public MalformedSelectException(String s) {
      super(s);
    }
  }
  
  public boolean equals(Object o) {
    if (o == null) return false;
    if (!(o instanceof QueryDefinition)) return false;
    return getName().equals(((QueryDefinition) o).getName());
  }
  
  public int hashCode() {
    return getName().hashCode();
  }
}
