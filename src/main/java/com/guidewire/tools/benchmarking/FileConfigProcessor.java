package com.guidewire.tools.benchmarking;

import com.guidewire.tools.DataExtractionUtils;
import java.io.InputStream;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;



class FileConfigProcessor implements ConfigProcessor {
  private final String _configFilePath;

  FileConfigProcessor(String file) {
    _configFilePath = file;
  }

  @Override
  public void process(DataExtractor dataExtractor) throws Exception {

    dataExtractor.setConfigFilePath(_configFilePath);
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setValidating(false);
    SAXParser parser = factory.newSAXParser();
    InputStream configurationFileInputStream = DataExtractionUtils.getInputStreamBasedOnFileName(_configFilePath);
    if (configurationFileInputStream == null) {
      dataExtractor.getDataExtractorLog().error("Couldn't open configuration file: " + _configFilePath);
    }
    else {
      parser.parse(configurationFileInputStream, new ConfigParserHandler(dataExtractor));
    }

  }
}

