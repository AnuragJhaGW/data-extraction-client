package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.client.DataExtractorClient;
import com.guidewire.util.TextReader;
import java.io.InputStream;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;


/**
 * Test BenchmarkingExtractorXMLConfig, the config file
 */
@Test(groups="unit")
public class ConfigParserHandlerTest {

  /**
   * Test default attributes in the config file
   */
  public void testDefaultAttributes() throws Exception {
    DataExtractor dataExtractor = dataExtractor("testDefaultAttributes");

    assertEquals(dataExtractor.getDBURL(), null);
    assertEquals(dataExtractor.getDB(), null);
    assertEquals(dataExtractor.getDBUserID(), null);
    assertEquals(dataExtractor.getDbUserPassword(), null);
    assertEquals(dataExtractor.getDBTag(), "");

    assertEquals(dataExtractor.getUploadURL(), "https://cbs.guidewire.com/dataExtractionServer");
    assertEquals(dataExtractor.getUsername(), null);
    assertEquals(dataExtractor.getGuidewirePassword(), null);
    assertEquals(dataExtractor.getGwAuth(), null);
    assertEquals(dataExtractor.getCasHost(), "gw-loginservice-prod.guidewire.net");
    assertEquals(dataExtractor.getDomain(), "cbs.guidewire.com");
  }


  /**
   * Test setting attributes in the config file
   */
  public void testAttributes() throws Exception {
    DataExtractor dataExtractor = dataExtractor("testAttributes");

    assertEquals(dataExtractor.getDBURL(), "test_rbcbxh");
    assertEquals(dataExtractor.getDB(), "test_vtkikb");
    assertEquals(dataExtractor.getDBUserID(), "test_yawpwf");
    assertEquals(dataExtractor.getDbUserPassword(), "test_maercn");
    assertEquals(dataExtractor.getDBTag(), "test_plikdo.");

    assertEquals(dataExtractor.getUploadURL(), "test_mofuoc");
    assertEquals(dataExtractor.getUsername(), "test_ezruuk");
    assertEquals(dataExtractor.getGuidewirePassword(), "test_bawqmr");
    assertEquals(dataExtractor.getGwAuth(), "test_jkdusa");
    assertEquals(dataExtractor.getCasHost(), "test_hlrwve");
    assertEquals(dataExtractor.getDomain(), "test_gliofp");

  }


  /**
   * If there's no "." in the dbTag, we add one
   */
  public void testNoDotInDbTag() throws Exception {
    DataExtractor dataExtractor = dataExtractor("testNoDotInDbTag");
    assertEquals(dataExtractor.getDBTag(), "test_mqhnkg.");
  }


  /**
   * Test authentication with the wallpaperupload userName/password in the config
   */
  public void testUserNamePasswordAuth() throws Exception {
    DataExtractor dataExtractor = dataExtractor("testUserNamePasswordAuth");


    TestDataExtractorClient dataExtractorClient = new TestDataExtractorClient(dataExtractor, 0, 0);
    assertNotNull(dataExtractorClient.getGwAuth());

  }


  /**
   * When gwAuth is specified in the config, the DataExtractorClient should use it
   */
  public void testGwAuthAttribute() throws Exception {
    DataExtractor dataExtractor = dataExtractor("testGwAuthAttribute");

    TestDataExtractorClient dataExtractorClient = new TestDataExtractorClient(dataExtractor, 0, 0);
    assertEquals(dataExtractorClient.getGwAuth(), "test_hlrwve");
  }


  /**
   * Get an exception if the config contains an unknown element.
   */
  public void testBadElementDetected() throws Exception {
    try {
      dataExtractor("testBadElementDetected");
      fail("expected an exception");
    }
    catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("test_bad_element"));
    }
  }


  /**
   * Get an exception if the config contains an unknown element.
   */
  public void testBadAttributesDetected() throws Exception {
    try {
      dataExtractor("testBadAttributesDetected");
      fail("expected an exception");
    }
    catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("test_bad_attribute1"));
      assertTrue(e.getMessage().contains("test_bad_attribute2"));
    }
  }


  public void testCaseInsensitive() throws Exception {
    DataExtractor dataExtractor = dataExtractor("testCaseInsensitive");
    assertEquals(dataExtractor.getDBUserID(), "test_yawpwf");
  }



  ////////////////////////   private

  /**
   * Create a DataExtractor by reading a configuration from BenchmarkingExtractorXMLConfigTest.txt.
   */
  private DataExtractor dataExtractor(String textReaderLabel) throws Exception {
    TextReader textReader = new TextReader("/BenchmarkingExtractorXMLConfigTest.txt");
    InputStream inputStream = textReader.getAsInputStream(textReaderLabel);

    DataExtractor dataExtractor = new DataExtractor();

    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setValidating(false);
    SAXParser parser = factory.newSAXParser();
    parser.parse(inputStream, new ConfigParserHandler(dataExtractor));

    return dataExtractor;

  }


  /**
   * Extend DataExtractorClient so we can make getGwAuth() public
   */
  private class TestDataExtractorClient extends DataExtractorClient {
    public TestDataExtractorClient(DataExtractor extractorToUse, long start, long maxRunTime) {
      super(extractorToUse, start, maxRunTime);

    }

    @Override
    public String getGwAuth() {
      return super.getGwAuth();
    }
  }
}

