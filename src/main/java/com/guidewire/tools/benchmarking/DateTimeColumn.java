package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.RowDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Class description...
 *
 * @author rvernick
 */
public class DateTimeColumn extends ColumnDef {
  private static final String YYYY_MM_DD = "yyyy-MM-dd";
  private static final String YYYYMMDD_HHMMSS_SSSZ = "yyyyMMdd HH:mm:ss.SSSZ";
  private static final String YYYY_MM_DD_HHMMSS_SSSZ = "yyyy-MM-dd HH:mm:ss.SSSZ";
  private static final String MM_DD_YYYY_HHMMSS_SSSZ = "MM/dd/yyyy HH:mm:ss.SSSZ";
  private static final String DDMMYYYY_HHMMSS_SS = "ddMMMyyyy:HH:mm:ss";
  private static final String YYYYMMDD_HHMMSS_SSS = "yyyyMMdd HH:mm:ss.SSS";
  private static final String YYYY_MM_DD_HHMMSS_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
  private static final String MM_DD_YYYY_HHMMSS_SSS = "MM/dd/yyyy HH:mm:ss.SSS";
  // This format string gets changed if a client specifies a different one. We store this in the CSVFileDefinition so that we maintain
  // the format string for a client in their config.
  private String formatString = YYYYMMDD_HHMMSS_SSSZ;
  // The default format is the one we expect to use for our output. So even if a client specifies a format, we only want to use that
  // format to parse the input. When we output the date as a string we want to use this format.
  private transient SimpleDateFormat defaultFormat = new SimpleDateFormat(formatString);
  private transient boolean useTimestampWithTimezone = false;
  private transient List<SimpleDateFormat> formats = new ArrayList<SimpleDateFormat>(10);

  public DateTimeColumn(String name, String type, String formattingString) {
    super(name, type);
    setUpDateFormatters(formattingString);
  }

  private void setUpDateFormatters(String formattingString) {
    defaultFormat = new SimpleDateFormat(formatString);
    if (formattingString != null && !"".equals(formattingString)) {
      formats.add(new SimpleDateFormat(formattingString));
      formatString = formattingString;
    }
    formats.add(defaultFormat);
    formats.add(new SimpleDateFormat(YYYYMMDD_HHMMSS_SSS));
    formats.add(new SimpleDateFormat(DDMMYYYY_HHMMSS_SS));
    formats.add(new SimpleDateFormat(YYYY_MM_DD));
    formats.add(new SimpleDateFormat(YYYY_MM_DD_HHMMSS_SSSZ));
    formats.add(new SimpleDateFormat(MM_DD_YYYY_HHMMSS_SSSZ));
    formats.add(new SimpleDateFormat(YYYY_MM_DD_HHMMSS_SSS));
    formats.add(new SimpleDateFormat(MM_DD_YYYY_HHMMSS_SSS));
  }

  public void setUseTimestamWithTimezone(boolean use) {
    useTimestampWithTimezone = use;
  }

  public boolean useTimestampWithTimezone() {
    return useTimestampWithTimezone;
  }

  @Override
  public String getOutputResults(ResultSet row) throws SQLException {
    Timestamp timestamp = row.getTimestamp(getName());
    if (row.wasNull()) {
      return "";
    }
    Date date = new Date(timestamp.getTime());
    return defaultFormat.format(date);
  }

  @Override
  public String getOutputResults(RowDataSource row) throws SQLException, ParseException {
    Timestamp timestamp = row.getTimestamp(getName());
    if (row.wasNull()) {
      return "";
    }
    Date date = new Date(timestamp.getTime());
    return defaultFormat.format(date);
  }

  @Override
  public String getDBColumnType() {
    return "DATETIME";
  }

  public Date getDate(String s) throws ParseException {
    if (wasNull(s)) return null;
    return dateFor(s);
  }

  @Override
  public Timestamp getTimestamp(String s) throws ParseException {
    if (wasNull(s)) return null;

    return new Timestamp(dateFor(s).getTime());
  }

  @Override
  public String getFormatString() {
    return formatString;
  }

  @Override
  public void setArgument(int i, PreparedStatement sql, String s) throws SQLException {
    try {
      if (useTimestampWithTimezone()) {
        // Note that when we're using timestamp with timezone, the prepared statement must
        // have the conversion built into it.  We assume that all date strings will include
        // a "-0x00" portion that indicates the timezone.
        sql.setString(i, s);
      } else {
        Timestamp timestamp = getTimestamp(s);
        if (timestamp == null) {
          sql.setNull(i, Types.TIMESTAMP);
        } else {
          sql.setTimestamp(i, timestamp);
        }
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public Date dateFor(String s) throws ParseException {
    Date result = null;
    ParseException latestParseException = null;
    for (SimpleDateFormat format : formats) {
      try {
        result = format.parse(s);
      } catch (ParseException pe) {
        latestParseException = pe;
      }
    }
    if (result == null) {
      throw latestParseException;
    }
    return result;
  }

  @Override
  public boolean isCompatible(ColumnDef expectedColumnDefinition) {
    return expectedColumnDefinition.isDateCompatible();
  }

  public boolean isDateCompatible() {
    return true;
  }

  protected List<SimpleDateFormat> getDateFormats() {
    return formats;
  }
}
