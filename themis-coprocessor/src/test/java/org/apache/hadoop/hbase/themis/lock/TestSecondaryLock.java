package org.apache.hadoop.hbase.themis.lock;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.junit.Test;

public class TestSecondaryLock extends TestThemisLock {

  @Test
  public void testWriteAndReadSecondaryLock() throws IOException {
    SecondaryLock expect = getSecondaryLock(COLUMN);
    SecondaryLock actual = new SecondaryLock();
    writeObjectToBufferAndRead(expect, actual);
    assertEquals(expect, actual);
  }

  @Test
  public void testToByteAndParseFromByte() throws IOException {
    SecondaryLock expect = getSecondaryLock(COLUMN);
    byte[] lockByte = ThemisLock.toByte(expect);
    ThemisLock actual = ThemisLock.parseFromByte(lockByte);
    assertEquals(expect, actual);
  }
}
