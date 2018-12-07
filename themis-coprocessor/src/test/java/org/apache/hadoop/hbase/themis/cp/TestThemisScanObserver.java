package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.themis.TestBase;
import org.junit.Test;

public class TestThemisScanObserver extends TestBase {

  @Test(expected = NullPointerException.class)
  public void testPreOpenHookWithException() throws IOException {
    TransactionTTL.init(HBaseConfiguration.create());
    ThemisScanObserver observer = new ThemisScanObserver();
    observer.preScannerOpen(null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testPostOpenHookWithException() throws IOException {
    TransactionTTL.init(HBaseConfiguration.create());
    ThemisScanObserver observer = new ThemisScanObserver();
    observer.postScannerOpen(null, null, null);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testNextHookWithException() throws IOException {
    TransactionTTL.init(HBaseConfiguration.create());
    ThemisScanObserver observer = new ThemisScanObserver();
    observer.preScannerNext(null, new ThemisServerScanner(null, Long.MAX_VALUE, null), null, 0,
      false);
  }
}