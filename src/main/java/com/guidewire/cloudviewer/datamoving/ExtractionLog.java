package com.guidewire.cloudviewer.datamoving;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Class description...
 */
public class ExtractionLog {
  private String errors = "";
  private String warnings = "";
  private String info = "";

  public ExtractionLog(InputStream logFile) {
    read(logFile);
  }

  public String getErrors() {
    return errors;
  }

  public String getWarnings() {
    return warnings;
  }

  public String getInfo() {
    return info;
  }

  public void trimToMaxSize(int maxSize) {
    if (errors.length() > maxSize) {
      errors = errors.substring(0, maxSize);
    }
    maxSize = maxSize - errors.length();
    
    if (warnings.length() > maxSize) {
      warnings = warnings.substring(0, Math.max(0, maxSize));
    }
    maxSize = maxSize - warnings.length();

    if (info.length() > maxSize) {
      info = info.substring(0, Math.max(0, maxSize));
    }
  }

  private void read(InputStream logFile) {
    BufferedReader reader = new BufferedReader(new InputStreamReader(new DataInputStream(logFile)));
    String line;
    StringBuffer errorsBuffer = new StringBuffer();
    StringBuffer warningsBuffer = new StringBuffer();
    StringBuffer infoBuffer = new StringBuffer();
    try {
      while ((line = reader.readLine()) != null) {
        if (line.contains("error") || line.contains("ERROR")) {
          errorsBuffer.append(line).append("\n");
        }
        if (line.contains("warning") || line.contains("WARNING")) {
          warningsBuffer.append(line).append("\n");
        }
        if (line.contains("info") || line.contains("INFO")) {
          infoBuffer.append(line).append("\n");
        }
      }
      errors = errorsBuffer.toString();
      warnings = warningsBuffer.toString();
      info = infoBuffer.toString();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
