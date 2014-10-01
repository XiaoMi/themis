package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.cp.ThemisScanObserver;
import org.junit.Assert;
import org.junit.Test;

public class TestThemisScanObserver extends TestBase {
  @Test
  public void testHookWithException() throws IOException {
    TransactionTTL.init(HBaseConfiguration.create());
    ThemisScanObserver observer =new ThemisScanObserver();
    try {
      observer.preScannerOpen(null, null, null);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getCause() instanceof NullPointerException);
    }
    
    try {
      observer.preScannerNext(null, new ThemisServerScanner(null, Long.MAX_VALUE), null, 0, false);
      Assert.fail();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(e.getCause() instanceof NullPointerException);
    }
  }
}
