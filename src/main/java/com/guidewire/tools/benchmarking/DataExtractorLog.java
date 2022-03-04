package com.guidewire.tools.benchmarking;


import com.guidewire.cloudviewer.datamoving.ExtractionException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang.time.DateUtils;



public class DataExtractorLog {
  private static final String LOG_FILE_PREFIX = "extract.";
  private static final String LOG_FILE_SUFFIX = ".log";
  private static final DateFormat _logNameDateformat = new SimpleDateFormat("yyyyMMdd-HHmmss");
  private static final DateFormat _formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS Z");

  private File _logFile;
  private BufferedWriter _logWriter;
  private String _logDir = "logs";
  private List<String> _errorMessages = new ArrayList<>();


  /**
   * Constructor with package access
   */
  DataExtractorLog() {}



  void createLogFile() {
    File logFile = getLogFile();
    try {
      FileWriter logFileWriter = new FileWriter(logFile, false);
      _logWriter = new BufferedWriter(logFileWriter);
    } catch (IOException e) {
      System.out.println("Unable to create logFileWriter: " + logFile.getAbsolutePath());
      e.printStackTrace();
    }
  }


  void closeLogFile() {
    try {
      if (_logWriter != null) {
        _logWriter.close();
        _logWriter = null;
      }
      if (_logFile != null) {
        _logFile = null;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  File getLogFile() {
    if (_logFile == null) {
      _logFile = defineLogFile();
    }
    return _logFile;
  }


  public List<File> findLogs(int numberOfFiles) {
    List<File> files = new ArrayList<>();
    File logDir = getLogDir();
    FilenameFilter logFilter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.startsWith(LOG_FILE_PREFIX)) {
          return true;
        }
        return false;
      }
    };
    String[] children = logDir.list(logFilter);
    List<String> childList = new ArrayList<>();
    for (String child : children)
      childList.add(child);
    Collections.sort(childList);
    Collections.reverse(childList);
    // The list is now sorted with the newest files (that is, the ones with the higher
    // timestamps) first.  We want to skip the first file, which is the log file currently
    // being written, and return the next n files.
    for (int i=1; i<=numberOfFiles; i++) {
      if (childList.size() > i) {
        files.add(new File(logDir, childList.get(i)));
      }
    }
    return files;
  }



  String getLogsDir() {
    return _logDir;
  }

  void setLogsDir(String newLogsDir) {
    verifyDirectoryExistsAndIsWritable(newLogsDir);
    _logDir = newLogsDir;
  }


  static void verifyDirectoryExistsAndIsWritable(String directory) {
    File dir = new File(directory);
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IllegalStateException("Cannot create directory: " + directory + " as: " + dir.getAbsolutePath());
    }
    if (!dir.canWrite()) {
      throw new IllegalStateException("Cannot write to directory: " + directory + " as: " + dir.getAbsolutePath());
    }
  }


  boolean hasErrorMessages() {
    return _errorMessages.isEmpty() == false;
  }


  public List<String> getErrorMessages() {
    return _errorMessages;
  }



  /////////////////////////       write to log or console

  public void info(String msg) {
    display("INFO: " + msg);
  }


  public void debug(String msg) {
    display("DEBUG: " + msg);
  }


  public void errorToLogMsgToConsole(String logMsg, String consoleMsg) {
    display("ERROR: " + logMsg, false);
    display("ERROR: " + consoleMsg);
    _errorMessages.add(consoleMsg);
  }

  public void error(String msg) {
    display("ERROR: " + msg);
    _errorMessages.add(msg);
  }

  public void error(Exception e) {
    ExtractionException exception = new ExtractionException("", e);
    display("ERROR: " + e.toString());
    _errorMessages.add(e.toString());
    for (String row : exception.getStackTrace().split("\n")) {
      display("ERROR:   " + row);
    }
  }

  public void errorToLogMsgToConsole(Exception e, String msg) {
    ExtractionException exception = new ExtractionException("", e);
    display("ERROR: " + e.toString(), false);
    for (String row : exception.getStackTrace().split("\n")) {
      display("ERROR:   " + row, false);
    }
    display("ERROR: " + msg, false);
    displayToStdOut(msg);
    _errorMessages.add(msg);
  }


  public void display(String msg, boolean writeToStdOut) {
    try {
      if (_logWriter != null) {
        _logWriter.append("[" + getTimestamp() + "] " + msg);
        _logWriter.append("\n");
        _logWriter.flush();
      }
    } catch (IOException e) {
      System.err.println("Could not write to log file " + e.getMessage());
    }
    if (writeToStdOut) {
      System.out.println(msg);
    }
  }


  public void displayToStdOut(String msg) {
    System.out.println(msg);
  }



  ////////////////////////////////////////////    Private


  private File defineLogFile() {
    return defineLogFile(0);
  }


  private File defineLogFile(int daysBack) {
    File logDir = getLogDir();
    File result = new File(logDir, LOG_FILE_PREFIX + _logNameDateformat.format(DateUtils.addDays(new Date(), -1 * daysBack)) + LOG_FILE_SUFFIX);
    if (!logDir.exists()) {
      result.mkdir();
    }
    return result;
  }


  private File getLogDir() {
    File logDir = new File(getLogsDir());
    logDir.mkdirs();
    return logDir;
  }


  private String getTimestamp() {
    Date now = new Date();
    return _formatter.format(now);
  }


  private void display(String msg) {
    display(msg, true);
  }
}
