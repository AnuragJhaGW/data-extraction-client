package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.TestToolCommands;
import com.guidewire.cloudviewer.datamoving.TestToolResults;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class calls various tools (e.g. tracert or tracerout) and records the information from
 * them in a TestToolResults object.
 */
public class TestConnectionTools {
  private static final long MAX_EXEC_TIME_MS = 2 * 60 * 1000;
  private static final long SLEEP_TIMEOUT = 5 * 1000;

  public static final String GWHOST = "&GWHOST";
  public static final String DBHOST = "&DBHOST";
  public static final String GW_URL = "&GW_URL";
  public static final String DB_URL = "&DB_URL";
  public static final String DB_DB = "&DB_DB";
  public static final String DB_USER = "&DB_USER";
  public static final String DB_TAG = "&DB_TAG";

  protected Boolean isWindows = null;
  protected TestToolResults info = null;

  String gwHost = null;
  String dbHost = null;

  public TestConnectionTools(TestToolResults ttrInfo) {
    info = ttrInfo;
    ttrInfo.setIsWindows(isWindows());
  }

  public void runTools(DataExtractor extractor, TestToolCommands commands) {
    for (String[] cmd : commands.getCommands()) {
      try {
        String[] finalCmd = replaceMacros(extractor, cmd);
        runCommand(finalCmd);
      } catch (IllegalArgumentException iae) {
        info.addOutput(cmd[0], "Error replacing macros in command [" + fullCommand(cmd) + "] error is [" + iae.getLocalizedMessage() + "]", 0L);
      }
    }
  }

  private String[] replaceMacros(DataExtractor extractor, String[] cmd) {
    cmd = replaceGWHost(extractor, cmd);
    cmd = replaceDBHost(extractor, cmd);
    cmd = replaceGWUrl(extractor, cmd);
    cmd = replaceDBUrl(extractor, cmd);
    cmd = replaceDBdb(extractor, cmd);
    cmd = replaceDBUser(extractor, cmd);
    cmd = replaceDBTag(extractor, cmd);

    return cmd;
  }

  private String[] replaceDBdb(DataExtractor extractor, String[] cmd) {
    return replaceMacro(cmd, DB_DB, extractor.getDB());
  }

  private String[] replaceGWUrl(DataExtractor extractor, String[] cmd) {
    return replaceMacro(cmd, GW_URL, extractor.getUploadURL());
  }

  private String[] replaceDBUrl(DataExtractor extractor, String[] cmd) {
    return replaceMacro(cmd, DB_URL, extractor.getDBURL());
  }

  private String[] replaceDBUser(DataExtractor extractor, String[] cmd) {
    return replaceMacro(cmd, DB_USER, extractor.getDBUserID());
  }

  private String[] replaceDBTag(DataExtractor extractor, String[] cmd) {
    return replaceMacro(cmd, DB_TAG, extractor.getDBTag());
  }

  private String[] replaceDBHost(DataExtractor extractor, String[] cmd) {
    if (!contains(cmd, DBHOST))
      return cmd;

    if (dbHost == null) {
      String dbUrl = extractor.getDBURL();
      int start = -1;
      int end = -1;
      if (dbUrl.toLowerCase().contains(DataExtractor.ORACLE)) {
        start = dbUrl.indexOf('@');
        if (start == -1)
          throw new IllegalArgumentException("could not extract database host from url [" + dbUrl + "], could not find start of host");
        else {
          start += 1;
          int nextSlash = dbUrl.indexOf('/', start);
          int nextColon = dbUrl.indexOf(':', start);
          end = lesserNonNegative(nextSlash, nextColon);
          if (end == -1)
            throw new IllegalArgumentException("could not extract database host from url [" + dbUrl + "], could not find end of host start = [" + start + "]");
        }
      } else if (dbUrl.toLowerCase().contains(DataExtractor.SQLSERVER)) {
        start = dbUrl.indexOf("//");
        if (start == -1)
          throw new IllegalArgumentException("could not extract database host from url [" + dbUrl + "], could not find start of host");
        else {
          start += 1;
          int nextBackslash = dbUrl.indexOf('\\', start);
          int nextColon = dbUrl.indexOf(':', start);
          end = lesserNonNegative(nextBackslash, nextColon);
          if (end == -1)
            throw new IllegalArgumentException("could not extract database host from url [" + dbUrl + "], could not find end of host start = [" + start + "]");
        }
      } else {
        throw new IllegalArgumentException("could not extract database host from url [" + dbUrl + "], could not determine database type");
      }
      dbHost = dbUrl.substring(start, end);
    }

    return replaceMacro(cmd, DBHOST, dbHost);
  }

  private String[] replaceGWHost(DataExtractor extractor, String[] cmd) {
    if (!contains(cmd, GWHOST))
      return cmd;

    // Figure out the replacement string
    if (gwHost == null) {
      String totalUrl = extractor.getUploadURL();
      int start = totalUrl.indexOf("//");
      if (start == -1) {
        throw new IllegalArgumentException("could not extract guidewire host from url [" + totalUrl + "]");
      } else {
        start += 2;
        int nextSlash = totalUrl.indexOf('/', start);
        int nextColon = totalUrl.indexOf(':', start);
        int end = lesserNonNegative(nextSlash, nextColon);
        gwHost = totalUrl.substring(start, end);
      }
    }

    return replaceMacro(cmd, GWHOST, gwHost);
  }

  private String[] replaceMacro(String[] cmd, String toReplace, String replacementValue) {
    String[] result = new String[cmd.length];
    for (int i=0; i<cmd.length; i++) {
      Pattern pattern = Pattern.compile(toReplace);
      final Matcher matcher = pattern.matcher(cmd[i]);
      result[i] = matcher.replaceAll(replacementValue);

    }
    return result;
  }

  private boolean contains(String[] cmd, String toReplace) {
    for (String s : cmd) {
      if (s.contains(toReplace))
        return true;
    }
    return false;
  }

  public String fullCommand(String[] cmd) {
    StringBuilder commandString = new StringBuilder();
    for (String str : cmd) {
      commandString.append(str);
      commandString.append(" ");
    }
    return commandString.toString();
  }

//  private List<String> runCommand(String[] cmd) {
//    List<String> result = new ArrayList<String>();
//    String command = fullCommand(cmd);
//    result.add("Executing command [" + command + "]");
//
//    String s = null;
//    try {
//      // Run the command
//      long start = System.currentTimeMillis();
//      Process p = Runtime.getRuntime().exec(cmd);
//
//      // Get the output
//      BufferedReader stdOut = new BufferedReader(new InputStreamReader(p.getInputStream()));
//      while ((s = stdOut.readLine()) != null) {
//        result.add(s);
//      }
//
//      // Get std err
//      BufferedReader stdErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
//      boolean first = true;
//      while ((s = stdErr.readLine()) != null) {
//        if (first) {
//          result.add("Errors from [" + command + "]");
//          first = false;
//        }
//        result.add(s);
//      }
//    } catch (IOException e) {
//      result.add("Error executing command [" + command + "]");
//    }
//    return result;
//  }

  private void runCommand(String[] cmd) {
    long start = System.currentTimeMillis();
    ExecutionThread et = new ExecutionThread(cmd);
    et.start();

    while (!et.isComplete()) {
      try {
        Thread.sleep(SLEEP_TIMEOUT);
        if (System.currentTimeMillis() - start > MAX_EXEC_TIME_MS) {
          // If the ExecutionThread is waiting for the process to complete, this
          // will interrupt it and cause the thread to complete.
          et.interrupt();
          // After interrupting, sleep for a moment to give the ExecutionThread object
          // time to finish
          Thread.sleep(50);
        }
      } catch (InterruptedException e) {
      }
    }
    long runTime = System.currentTimeMillis() - start;

    info.addOutput(cmd[0], et.getResults(), runTime);
  }

    // We will execute the command in a separate thread.  This is because we will be blocking while we
  // wait for the process to complete.  But we want to have a maximum timeout for any given command, to
  // protect us from bad commands, or some vagarity of the machine we're running on which may cause some
  // command to never complete, or take a very long time.  So we will run the command in a separate thread
  // and monitor it, killing it if necessary
  private class ExecutionThread extends Thread {
    protected String[] cmd;
    protected List<String> results = new ArrayList<String>();
    protected String command;
    protected boolean complete = false;

    ExecutionThread(String[] cmd) {
      this.cmd = cmd;
      command = fullCommand(cmd);
    }

    public List<String> getResults () {
      return results;
    }

    public boolean isComplete() {
      return complete;
    }

    public void run() {
      results.add("Executing command [" + command + "]");
      try {
        Process p = Runtime.getRuntime().exec(cmd);
        StreamReader stdOut = new StreamReader(p.getInputStream(), command);
        StreamReader stdErr = new StreamReader(p.getErrorStream(), command);
        stdOut.start();
        stdErr.start();

        int status = 0;
        try {
          status = p.waitFor();
        } catch (InterruptedException e) {
          // Note that we handle the InterruptedException here, so that we
          // can get whatever results we might already have.
          results.add("Process [" + command + "] has been interrupted");
        }
        results.addAll(stdOut.getResults());
        if (stdErr.getResults().size() > 0) {
          results.add("Errors from [" + command + "]");
          results.addAll(stdErr.getResults());
        }
        results.add("process status [" + status + "]");
      } catch (IOException e) {
        results.add("Error executing command [" + command + "] error [" + e.getLocalizedMessage() + "]");
      }
      complete = true;
    }
  }

  // We want to handle reading from stdout and stderr in separate threads, since we want to block
  // waiting for the process to complete.  But if we block and read in the same thread, we are
  // vulnerable to process that write more than will fit in the output buffers, and we can either
  // end up blocking forever waiting for the buffers to be cleared, or we may lose data if the
  // buffers flush.  In either case, we want to read from the buffers in separate threads.
  private class StreamReader extends Thread {
    InputStream is;
    protected List<String> results = new ArrayList<String>();
    String command = null;

    StreamReader (InputStream stream, String cmd) {
      is = stream;
      command = cmd;
    }

    List<String> getResults () {
      return results;
    }

    public void run() {
      if (is != null) {
        String s = null;
        BufferedReader stdOut = new BufferedReader(new InputStreamReader(is));
        try {
          while ((s = stdOut.readLine()) != null) {
            results.add(s);
          }
        } catch (IOException e) {
          results.add("Error executing command [" + command + "] error [" + e.getLocalizedMessage() + "]");
        }
      }
    }
  }

  public boolean isWindows() {
    if (isWindows == null) {
      // If we've never been called before, check
      // This check may not be great for all possible systems (i.e. mobile phones reportedly
      // fail with it).  But for what we should see, it should work.
      File[] roots = File.listRoots();
      if (roots.length == 1 && "/".equals(roots[0].getAbsolutePath()))
        isWindows = Boolean.FALSE;
      else
        isWindows = Boolean.TRUE;
    }

    return isWindows;
  }

  private int lesserNonNegative(int first, int second) {
    int result = -1;
    if (first == -1)
      result = second;
    else if (second == -1) {
      result = first;
    } else {
      result = second < first ? second : first;
    }
    return result;
  }
}
