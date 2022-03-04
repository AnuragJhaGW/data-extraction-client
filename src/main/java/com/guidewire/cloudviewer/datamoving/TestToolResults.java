package com.guidewire.cloudviewer.datamoving;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is used to capture information during a test connection about the client system, network, etc.
 * We expect that this will contain result information from a set of tools that are run on the system, for
 * example, traceroute or tracert.
 */
public class TestToolResults {
  /**
   * toolOutput contains the results from running a set of tools.  The name of the tool is the key
   * to the map, and the output from the tool is the value.
   */
  protected Map<String, List<String>> toolOutput = new HashMap<String, List<String>>();
  protected Map<String, Long> toolPerf = new HashMap<String, Long>();
  boolean isWindows;

  public Map<String, List<String>> getToolOutput() {
    return toolOutput;
  }

  public Map<String, Long> getToolPerf() {
    return toolPerf;
  }

  public void addOutput(String tool, String output, Long perf) {
    List<String> outputList = new ArrayList<String>();
    outputList.add(output);
    toolOutput.put(tool, outputList);
    toolPerf.put(tool, perf);
  }

  public void addOutput(String tool, List<String> output, Long perf) {
    toolOutput.put(tool, output);
    toolPerf.put(tool, perf);
  }

  public void setIsWindows(boolean windows) {
    isWindows = windows;
  }
}
