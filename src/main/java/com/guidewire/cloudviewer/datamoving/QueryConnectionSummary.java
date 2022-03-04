package com.guidewire.cloudviewer.datamoving;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class encapsulates information about a single client interaction with the dataExtractionServer.  It is
 * intended to collect information which will then be sent to the
 *
 */
public class QueryConnectionSummary {
  protected String username;
  // Note that starttime and endtime are simple UTC milliseconds, as reported by the machine running the
  // client.  Since the clock on the client may or may not be well synced, we should only rely on the difference
  // between the two values being meaningful.
  protected Long starttime;
  protected Long endtime;

  protected Integer totalRowsSent = 0;

  /**
   * queryInfo contains an entry for every query, with the key being the query name and the value
   * being the
   */
  protected Map<String, Integer> queryInfo;

  protected List<String> messages;


  public QueryConnectionSummary(String username) {
    this.username = username;
    starttime = System.currentTimeMillis();
    queryInfo = new HashMap<String, Integer>();
    messages = new ArrayList<String>();
  }

  public String getUsername() {
    return username;
  }

  public Long getConnectionDuration() {
    return endtime - starttime;
  }

  public Map<String, Integer> getQueryInfo() {
    return queryInfo;
  }

  public void addQueryInfo(String query, Integer rows) {
    int currentTotal = 0;
    if (queryInfo.get(query) != null) {
      currentTotal = queryInfo.get(query);
    }

    queryInfo.put(query, rows + currentTotal);
    totalRowsSent += rows;
  }

  public Integer getTotalRowsSent() {
    return totalRowsSent;
  }

  public void setTotalRowsSent(Integer totalRowsSent) {
    this.totalRowsSent = totalRowsSent;
  }

  public void setEndtime(Long endtime) {
    this.endtime = endtime;
  }

  public List<String> getMessages() {
    return messages;
  }

  public void addMessage(String message) {
      messages.add(message);
  }
}
