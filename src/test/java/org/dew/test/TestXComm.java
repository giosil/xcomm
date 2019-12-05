package org.dew.test;

import org.dew.xcomm.ws.WSMessages;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestXComm extends TestCase {
  
  public TestXComm(String testName) {
    super(testName);
  }
  
  public static Test suite() {
    return new TestSuite(TestXComm.class);
  }
  
  public void testApp() {
    System.out.println(WSMessages.class.getCanonicalName() + " " + WSMessages.VERSION);
  }
  
}
