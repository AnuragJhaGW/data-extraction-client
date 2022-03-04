package com.guidewire.tools.benchmarking;

import org.testng.annotations.Test;
import static org.testng.Assert.*;


@Test(groups="unit")
public class DataExtractorRunnerTest {


  /**
   * If client is in the command line args, make sure it gets set in the DataExtractor.
   */
  public void testGetExtractor_ClientArg() throws Exception {

    String[] args = {
      "-client", "torus"
    };

    DataExtractionRunner dataExtractionRunner = new DataExtractionRunner(args) {
      // override this so we don't have to provide a config file
      @Override
      ConfigProcessor getConfigProcessor() {
        return new ConfigProcessor() {
          @Override
          public void process(DataExtractor dataExtractor) throws Exception {
            // don't set username in the DataExtractor
          }
        };
      }
    };

    DataExtractor dataExtractor = dataExtractionRunner.getDataExtractor();
    assertEquals(dataExtractor.getClient(), "torus");
  }


  /**
   * If client is in the command line args, make sure it gets set in the DataExtractor.
   */
  public void testGetExtractor_UserNameInConfig() throws Exception {
    String[] args = { };

    DataExtractionRunner dataExtractionRunner = new DataExtractionRunner(args) {
      // override this so we don't have to provide a config file
      @Override
      ConfigProcessor getConfigProcessor() {
        return new ConfigProcessor() {
          @Override
          public void process(DataExtractor dataExtractor) throws Exception {
            // set client in the DataExtractor
            dataExtractor.setUsername("foo");
          }
        };
      }
    };

    DataExtractor dataExtractor = dataExtractionRunner.getDataExtractor();
    assertEquals(dataExtractor.getClient(), null);
  }


  /**
   * If the config file doesn't have a username, then the command line args must
   * have -client
   */
  public void testGetExtractor_ClientArgRequiredWhenNoUsername() throws Exception {
    try {
      String[] args = { };
      new DataExtractionRunner(args) {
        @Override
        ConfigProcessor getConfigProcessor() {
          return new ConfigProcessor() {
            @Override
            public void process(DataExtractor dataExtractor) throws Exception {
              // don't set username in the DataExtractor
            }
          };
        }
      };

      fail("Expect exception");
    }
    catch (Exception e) {
      assertTrue(e.getMessage().contains("you must set the -client command line argument"));

    }
  }


  /**
   * If the config file has a username, then the command line args must not
   * have -client
   */
  public void testGetExtractor_ClientArgForbiddenWhenThereIsAUsername() throws Exception {

    String[] args = {
      "-client", "torus"
    };


    try {
      new DataExtractionRunner(args) {
        @Override
        ConfigProcessor getConfigProcessor() {
          return new ConfigProcessor() {
            @Override
            public void process(DataExtractor dataExtractor) throws Exception {
              // set client in the DataExtractor
              dataExtractor.setUsername("foo");
            }
          };
        }
      };

      fail("Expect exception");
    }
    catch (Exception e) {
      assertTrue(e.getMessage().contains("you can't set the -client command line argument. -client=torus"));
    }
  }

}

