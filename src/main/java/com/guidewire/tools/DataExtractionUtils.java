package com.guidewire.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Class description...
 */
public class DataExtractionUtils {
  private static DataExtractionUtils instance = new DataExtractionUtils();
  
  public static InputStream getInputStreamBasedOnFileName(String fileName) throws FileNotFoundException {
    return instance.findInputStreamBasedOnFileName(fileName);
  }
  
  private InputStream findInputStreamBasedOnFileName(String fileName) throws FileNotFoundException {
    if (fileName == null) {
      return null;
    }
    File inputFile = new File(fileName);
    if (inputFile.exists()) {
      return new FileInputStream(inputFile);
    }
    return getClass().getClassLoader().getResourceAsStream(fileName);
  }

}
