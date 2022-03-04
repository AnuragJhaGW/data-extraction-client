package com.guidewire.cloudviewer.datamoving;

import com.guidewire.tools.benchmarking.CSVFileDefinition;
import com.guidewire.tools.benchmarking.QueryDefinition;

import java.util.List;

/**
* Class description...
*/
public class CSVFileDefinitionJSON {
    private List<CSVFileDefinition> definitions;

    public void setFileDefinitions(List<CSVFileDefinition> list) {
      definitions = list;
    }

  public List<CSVFileDefinition> getFileDefinitions() {
    return definitions;
  }
}
