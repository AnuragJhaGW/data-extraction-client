package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.TestToolCommands;
import com.guidewire.cloudviewer.datamoving.TestToolResults;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Test to check that the mechanism for running connection tools sent from the server works correctly
 */
//@Test(groups="unit")
public class TestConnectionToolsTest {
  private static final String TEST_CONFIG_CC6_MSSQL_XML = "test_config_cc6_mssql.xml";

  /**
   * Basic test of functionality, using a simple ping command
   */
  public void pingTest() throws Exception {
    DataExtractor extractor = new DataExtractor(TEST_CONFIG_CC6_MSSQL_XML, "TestingClient", DataExtractionRunner.PHRASE);
    TestToolResults info = new TestToolResults();
    TestConnectionTools tool = new TestConnectionTools(info);

    // Set up the commands to run
    List<String[]> commandStrings = new ArrayList<>();
    if (tool.isWindows()) {
      commandStrings.add(new String[] {"ping", "-n", "4", "vpn5.guidewire.com"});
    } else {
      commandStrings.add(new String[] {"ping", "-c", "4", "vpn5.guidewire.com"});
    }
    TestToolCommands commands = new TestToolCommands(commandStrings, tool.isWindows());

    // Run the commands
    tool.runTools(extractor, commands);

    // Check the output
    Map<String, List<String>> output = info.getToolOutput();
    Assert.assertEquals("output contains [" + output.size() + "] entries, should have 1", 1, output.size());
    List<String> pingResults = output.get("ping");
    Assert.assertNotNull("Found no results for ping command", pingResults);
    // The last line should be process status
    Assert.assertEquals("Incorrect process status", "process status [0]", pingResults.get(pingResults.size()-1));
    // We need to do different checking for windows vs linux, since the ping command returns different results
    if (tool.isWindows()) {
      // Find the status line
      for (int i=pingResults.size()-1; i>=0; i--) {
        if (pingResults.get(i).contains("Packets")) {
          String statusLine = pingResults.get(i);
          int start = statusLine.indexOf("Sent =") + 7;
          int end = statusLine.indexOf(",", start);
          String numSentString = statusLine.substring(start, end).trim();
          int numSent = Integer.parseInt(numSentString);
          Assert.assertEquals("Incorrect number of sent packets got [" + numSent + "] expected 4", 4, numSent);

          start = statusLine.indexOf("Received =") + 11;
          end = statusLine.indexOf(",", start);
          String numReceivedString = statusLine.substring(start, end).trim();
          int numReceived = Integer.parseInt(numReceivedString);
          Assert.assertEquals("Incorrect number of received packets got [" + numReceived + "] expected 4", 4, numReceived);

          start = statusLine.indexOf("Lost =") + 7;
          end = statusLine.indexOf("(", start);
          String numLostString = statusLine.substring(start, end).trim();
          int numLost = Integer.parseInt(numLostString);
          Assert.assertEquals("Incorrect number of lost packets got [" + numLost + "] expected 0", 0, numLost);
          break;
        }
      }
    } else {
      // Find the status line
      for (int i=pingResults.size()-1; i>=0; i--) {
        if (pingResults.get(i).contains("packets")) {
          String statusLine = pingResults.get(i);
          int start = 0;
          int end = statusLine.indexOf("packets transmitted");
          String numSentString = statusLine.substring(start, end).trim();
          int numSent = Integer.parseInt(numSentString);
          Assert.assertEquals("Incorrect number of sent packets got [" + numSent + "] expected 4", 4, numSent);

          start = statusLine.indexOf(",") + 1;
          end = statusLine.indexOf("received", start);
          String numReceivedString = statusLine.substring(start, end).trim();
          int numReceived = Integer.parseInt(numReceivedString);
          Assert.assertEquals("Incorrect number of received packets got [" + numReceived + "] expected 4", 4, numReceived);

          start = statusLine.indexOf("received") + 9;
          end = statusLine.indexOf("%", start);
          String numLostString = statusLine.substring(start, end).trim();
          int numLost = Integer.parseInt(numLostString);
          Assert.assertEquals("Incorrect packet loss got [" + numLost + "] expected 0", 0, numLost);
          break;
        }
      }
    }
  }

  /**
   * Test to make sure that timeout works correctly.  We will use a ping command that sends a large enough
   * number of pings that it won't complete withing the timeout window.
   */
  public void timeoutTest() throws Exception {
    DataExtractor extractor = new DataExtractor(TEST_CONFIG_CC6_MSSQL_XML, "TestingClient", DataExtractionRunner.PHRASE);
    TestToolResults info = new TestToolResults();
    TestConnectionTools tool = new TestConnectionTools(info);

    // Set up the commands to run
    List<String[]> commandStrings = new ArrayList<>();
    if (tool.isWindows()) {
      commandStrings.add(new String[] {"ping", "-n", "400", "vpn5.guidewire.com"});
    } else {
      commandStrings.add(new String[] {"ping", "-c", "400", "vpn5.guidewire.com"});
    }
    TestToolCommands commands = new TestToolCommands(commandStrings, tool.isWindows());

    // Run the commands
    tool.runTools(extractor, commands);

    // Check the output.  First, basic tests
    Map<String, List<String>> output = info.getToolOutput();
    Assert.assertEquals("output contains [" + output.size() + "] entries, should have 1", 1, output.size());
    List<String> pingResults = output.get("ping");
    Assert.assertNotNull("Found no results for ping command", pingResults);

    // Check that we got the message about having interrupted the process
    String firstLine = pingResults.get(1);
    Assert.assertEquals("Didn't find the process interruption message, found [" + firstLine + "]", "Process [" + tool.fullCommand(commandStrings.get(0)) + "] has been interrupted", firstLine);
  }
}
