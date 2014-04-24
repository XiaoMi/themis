package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import org.apache.hadoop.hbase.themis.ThemisScan;
import org.apache.hadoop.hbase.themis.TestBase;
import org.junit.Assert;
import org.junit.Test;

public class TestThemisScan extends TestBase {
  @Test
  public void testSetMethodsOfScan() throws IOException {
    ThemisScan scan = new ThemisScan();
    scan.setStartRow(ROW);
    scan.setStopRow(ROW);
    scan.setCaching(1000);

    Assert.assertArrayEquals(ROW, scan.getStartRow());
    Assert.assertArrayEquals(ROW, scan.getStopRow());
    Assert.assertEquals(1000, scan.getCaching());
  }
}
