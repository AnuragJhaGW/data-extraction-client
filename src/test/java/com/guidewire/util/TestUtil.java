package com.guidewire.util;

import com.guidewire.tools.benchmarking.DataExtractor;
import com.guidewire.tools.benchmarking.QueryDefinition;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class TestUtil {

  private static final String _alphabet = "abcdefghijklmnopqrstuvwxyz";
  private static final int _alphabetLength = _alphabet.length();
  private static final Random _random = new Random();


  public static String randomString() {
    StringBuilder sb = new StringBuilder();
    sb.append("test_");
    for (int i = 0; i < 6; i++) {
      sb.append(_alphabet.charAt(_random.nextInt(_alphabetLength)));
    }
    return sb.toString();
  }




  /**
   * Used to set values from an inner class.
   */
  public static class ValueHolder<T>  {
    public T value;

    public ValueHolder(T value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value == null ? "null" : value.toString();
    }
  }


  public static final String QUERY_CONFIG_FILE = "query_config_new.xml";

  public static QueryDefinition getQueryDefinition(String name) throws Exception {
    DataExtractor extractor = new DataExtractor();
    extractor.setQueryConfigFileName(QUERY_CONFIG_FILE);
    extractor.readQueries();
    return getQueryDefinition(extractor.getQueries(), name);
  }

  public static QueryDefinition getQueryDefinition(List<QueryDefinition> queries, String name) throws Exception {
    List<QueryDefinition> filteredQueryDefinitions = queries.stream()
            .filter(qd -> qd.getName().equals(name))
            .collect(Collectors.toList());
    return filteredQueryDefinitions.size() == 1 ? filteredQueryDefinitions.get(0) : null;
  }

  public static boolean hasQueryDefinition(String name) throws Exception {
    DataExtractor extractor = new DataExtractor();
    extractor.setQueryConfigFileName(QUERY_CONFIG_FILE);
    extractor.readQueries();
    return extractor.getQueries().stream()
            .filter(qd -> qd.getName().equals(name))
            .collect(Collectors.toList()).size() == 1;
  }
}
