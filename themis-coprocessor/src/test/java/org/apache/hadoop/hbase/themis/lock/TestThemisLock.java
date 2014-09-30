package org.apache.hadoop.hbase.themis.lock;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.junit.Test;

public class TestThemisLock extends TestBase {
  class NullThemisLock extends ThemisLock {
    @Override
    public boolean isPrimary() {
      return false;
    }
  }
  
  protected ThemisLock getNullThemisLock() {
    ThemisLock lock = (NullThemisLock)(new NullThemisLock());
    setThemisLock(lock, PREWRITE_TS);
    return lock;
  }
  
  @Test
  public void testWriteAndReadThemisLock() throws IOException {
    ThemisLock expect = getNullThemisLock();
    ThemisLock actual = new NullThemisLock();
    writeObjectToBufferAndRead(expect, actual);
    Assert.assertTrue(expect.equals(actual));
  }
}
