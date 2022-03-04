package com.guidewire.cloudviewer.datamoving;

import com.guidewire.tools.benchmarking.QueryDefinition;

import java.util.List;

/**
* Class description...
*/
public class QueryDefinitionJSON {
    private List<QueryDefinition> definitions;

    public void setQueries(List<QueryDefinition> list) {
      definitions = list;
    }

  public List<QueryDefinition> getQueries() {
    return definitions;
  }
}
