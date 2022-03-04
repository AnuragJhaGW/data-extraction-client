package com.guidewire.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Reads chunks of text from  a resource file
 */
public class TextReader {
  private final static String DEFAULT_LABEL_MARKER = "LABEL---------------------->";

  private final Map<String,String> _stringsMap; // immutable


  public TextReader(String file) {
    this(file, DEFAULT_LABEL_MARKER, true);
  }

  private TextReader(String file, String labelMarker, boolean trim) {
    try {
      Map<String,String> tempMap = readFile(file, labelMarker, trim);
      _stringsMap = Collections.unmodifiableMap(tempMap);
    }
    catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }


  /**
   * Returns the string for the given label.
   */
  public String get(String label) {
    String s = _stringsMap.get(label);
    if (s == null) {
      throw new IllegalArgumentException("Not found: " + label);
    }

    return s;
  }


  /**
   * Returns an InputStream for the given label.
   */
  public InputStream getAsInputStream(String label) {
    String s = get(label);
    return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
  }


  //////////////////////////////////////   Private

  private static Map<String,String> readFile(String file, String labelMarker, boolean trim)
    throws IOException {

    Map<String,String> labelToTextMap = new HashMap<>();

    InputStream inputStream = TextReader.class.getResourceAsStream(file);
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

    String label = null;
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);


    while (true) {
      String line = reader.readLine();

      if (line == null || line.startsWith(labelMarker)) {
        // finish the last chunk
        if (label != null) {
          String chunk = stringWriter.toString();
          if (trim) {
            chunk = chunk.trim();
          }
          labelToTextMap.put(label, chunk);
        }
      }

      if (line == null) {
        break; // done
      }

      else if (line.startsWith(labelMarker)) {
        // start new chunk
        label = line.substring(labelMarker.length(), line.length())
          .trim();
        stringWriter.getBuffer().setLength(0);
      }

      else {
        if (label == null) {
          throw new IllegalStateException("File needs to start with a label marker");
        }
        printWriter.println(line);
      }
    }

    return labelToTextMap;
  }

}
