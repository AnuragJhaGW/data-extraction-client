package com.guidewire.cloudviewer.datamoving.client;

import com.guidewire.tools.benchmarking.DataExtractor;
import com.guidewire.util.TestUtil;
import java.util.ArrayList;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.NameValuePair;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;


@Test(groups="unit")
public class DataExtractorClientTest {


  /**
   * If the DataExtractor has a client string set, it should get set in the post params
   */
  public void testSetParameters_ClientSetInQueryString() throws Exception {
    // mock DataExtractor
    final String client = "torus";
    DataExtractor dataExtractor = mock(DataExtractor.class);
    when(dataExtractor.getClient()).thenReturn(client);

    // mock httpMethod.setQueryString to test that we got the client param
    final TestUtil.ValueHolder<NameValuePair> nameValuePairHolder = new TestUtil.ValueHolder<>(null);
    HttpMethod httpMethod = mock(HttpMethod.class);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object[] arguments = invocationOnMock.getArguments();
        NameValuePair[] nameValuePairs = (NameValuePair[]) arguments[0];
        nameValuePairHolder.value = nameValuePairs[0];
        return null;
      }
    }).when(httpMethod).setQueryString(any(NameValuePair[].class));


    DataExtractorClient dataExtractorClient = new DataExtractorClient(dataExtractor, 0, 0);
    dataExtractorClient.setParameters(httpMethod, new ArrayList<NameValuePair>());

    assertEquals(nameValuePairHolder.value.getName(), "client");
    assertEquals(nameValuePairHolder.value.getValue(), client);
  }

}

