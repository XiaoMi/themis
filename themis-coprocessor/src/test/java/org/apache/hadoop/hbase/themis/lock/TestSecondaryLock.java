package org.apache.hadoop.hbase.themis.lock;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.themis.lock.SecondaryLock;
import org.junit.Test;

public class TestSecondaryLock extends TestThemisLock {
  @Test
  public void testWriteAndReadSecondaryLock() throws IOException {
    SecondaryLock expect = getSecondaryLock(COLUMN);
    SecondaryLock actual = new SecondaryLock();
    writeObjectToBufferAndRead(expect, actual);
    Assert.assertTrue(expect.equals(actual));
  }
  
  @Test
  public void testToByteAndParseFromByte() throws IOException {
    SecondaryLock expect = getSecondaryLock(COLUMN);
    byte[] lockByte = ThemisLock.toByte(expect);
    ThemisLock actual = ThemisLock.parseFromByte(lockByte);
    Assert.assertTrue(expect.equals(actual));
  }
}
