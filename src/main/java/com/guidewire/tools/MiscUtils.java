package com.guidewire.tools;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MiscUtils {

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
   */
  public static String transform(String stringToUpdate, Properties properties) {
    // Iterate properties and perform a regex replaceAll for each key/value pair.
    final Enumeration<?> keys = properties.propertyNames();
    while (keys.hasMoreElements()) {
      String key = (String) keys.nextElement();
      Pattern pattern = Pattern.compile(key);
      String target = properties.getProperty(key);
      final Matcher matcher = pattern.matcher(stringToUpdate);
      stringToUpdate = matcher.replaceAll(target);
    }
    return stringToUpdate;
  }


  /**
   * Get the stack trace from a Throwable
   */
  public static String stackTrace(Throwable t) {
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    t.printStackTrace(printWriter);
    return stringWriter.toString();
  }




  /**
   * Convenience method to turn keys and values into a String like:
   *   key1=val1, key2=val2, ...
   * We special case some formatting for example Dates and byte[]
   */
  public static String keyValueString(Object... keysAndValues) {
    if (keysAndValues == null) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (int keyIndex = 0; keyIndex < keysAndValues.length; keyIndex += 2) {
      if (!first) {
        sb.append(", ");
      }
      first = false;

      String key = (String) keysAndValues[keyIndex]; // it better be a string
      sb.append(key); // key
      sb.append("=");
      Object value = keysAndValues[keyIndex + 1];

      // convert value to a String
      String sValue;
      if (value == null) {
        sValue = "null";
      }

      else if (value instanceof byte[]) {
        sValue = Base64.getEncoder().encodeToString((byte[]) value);
      }

      else {
        sValue = value.toString();
      }

      sb.append(sValue);
    }

    return sb.toString();
  }

  public static int ccVersion(String sVersion) {
    sVersion = sVersion.trim();
    if (sVersion.toLowerCase().startsWith("cc")) {
      sVersion = sVersion.substring(2);
    }
    return Integer.parseInt(sVersion);
  }
}
