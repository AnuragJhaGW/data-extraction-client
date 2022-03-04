package com.guidewire.cloudviewer.datamoving;

import com.guidewire.tools.benchmarking.ColumnDef;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description...
 */
public class QueryResult {
  String name;
  List<ColumnDef> columns;
  List<ResultRow> rows = new ArrayList<ResultRow>();
  private Long checksum;
  private int rowCount;
  private boolean wasCutShort = false;
  private boolean lakeOnly = true;
  private Long queryTime;

  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }

  public List<ColumnDef> getColumns() {
    return columns;
  }
  public void setColumns(List<ColumnDef> cols) {
    columns = cols;
  }

  public List<ResultRow> getRows() {
    return rows;
  }
  public void setRows(List<ResultRow> rows) {
    this.rows = rows;
  }

  public int getRowCount() {
    return rowCount;
  }
  public void setRowCount(int count) {
    rowCount = count;
  }

  public boolean wasCutShort() {
    return wasCutShort;
  }
  public void setWasCutShort(boolean cutShort) {
    wasCutShort = cutShort;
  }

  public boolean isLakeOnly() {
    return lakeOnly;
  }
  public void setLakeOnly(boolean lakeOnly) {
    this.lakeOnly = lakeOnly;
  }

  public Long getChecksum() {
    return checksum;
  }

  public void setChecksum(Long val) {
    checksum = val;
  }

  public Long getQueryTime() {
    return queryTime;
  }

  public void setQueryTime(Long val) {
    queryTime = val;
  }

  public static ResultRow newResultRow(int columns) {
    return new ResultRow(columns);
  }

  public void addRow(ResultRow resultRow) {
    rows.add(resultRow);
  }

  public static class ResultRow {
    List<String> results;

    public ResultRow(int columns) {
      results = new ArrayList<String>(columns);
    }

    public List<String> getResults() {
      return results;
    }

    public void setResults(List<String> results) {
      this.results = results;
    }

    public void add(String outputResults) {
      results.add(outputResults);
    }
  }
}
