package org.apache.hadoop.hbase.themis;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.junit.Test;

public class TestThemisScan extends TestBase {

  @Test
  public void testSetMethodsOfScan() throws IOException {
    ThemisScan scan = new ThemisScan();
    scan.setStartRow(ROW);
    scan.setStopRow(ROW);
    scan.setCaching(1000);

    assertArrayEquals(ROW, scan.getStartRow());
    assertArrayEquals(ROW, scan.getStopRow());
    assertEquals(1000, scan.getCaching());
  }
}
