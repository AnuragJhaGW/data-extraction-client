package com.guidewire.tools;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Class description...
 */
public class PropertiesUtils {
  public static Properties getProperties(InputStream in) throws IOException {
    return getProperties(new Properties(), in);
  }

  public static Properties getProperties(Properties properties, InputStream in) throws IOException {
    properties.load(in);
    return properties;
  }

  public static Properties getProperties(String fileName) throws IOException {
    if (fileName == null) {
      return null;
    }
    return getProperties(new FileInputStream(fileName));
  }
}
