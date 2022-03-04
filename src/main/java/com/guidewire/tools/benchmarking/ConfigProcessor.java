package com.guidewire.tools.benchmarking;



/**
 * Processes config file, setting values in dataExtractor.  We have this so we can mock out the
 * config file processing in tests.
 */
public interface ConfigProcessor {
  void process(DataExtractor dataExtractor) throws Exception;

}
