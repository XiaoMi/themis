package org.apache.hadoop.hbase.themis.lock;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.themis.TestBase;
import org.junit.Test;

public class TestThemisLock extends TestBase {
  class NullThemisLock extends ThemisLock {
    @Override
    public boolean isPrimary() {
      return false;
    }
  }

  private ThemisLock getNullThemisLock() {
    ThemisLock lock = (NullThemisLock) (new NullThemisLock());
    setThemisLock(lock, PREWRITE_TS);
    return lock;
  }

  @Test
  public void testWriteAndReadThemisLock() throws IOException {
    ThemisLock expect = getNullThemisLock();
    ThemisLock actual = new NullThemisLock();
    writeObjectToBufferAndRead(expect, actual);
    assertEquals(expect, actual);
  }
}
