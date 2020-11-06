package org.apache.hadoop.hbase.themis.lock;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.Cell;
import org.junit.Test;

public class TestPrimaryLock extends TestThemisLock {
  @Test
  public void testAddSecondaryColumn() throws IOException {
    PrimaryLock lock = new PrimaryLock();
    lock.addSecondaryColumn(COLUMN, Cell.Type.Put);
    Assert.assertEquals(Cell.Type.Put, lock.getSecondaryColumn(COLUMN));
    lock.addSecondaryColumn(COLUMN, Cell.Type.DeleteColumn);
    Assert.assertEquals(Cell.Type.DeleteColumn, lock.getSecondaryColumn(COLUMN));
    lock.addSecondaryColumn(COLUMN_WITH_ANOTHER_TABLE, Cell.Type.Put);
    Assert.assertEquals(2, lock.getSecondaryColumns().size());
  }
  
  @Test
  public void testWriteAndReadPrimaryLock() throws IOException {
    PrimaryLock expect = getPrimaryLock();
    ThemisLock actual = new PrimaryLock();
    writeObjectToBufferAndRead(expect, actual);
    Assert.assertTrue(expect.equals(actual));
  }
  
   @Test
  public void testToByteAndParseFromByte() throws IOException {
    PrimaryLock expect = getPrimaryLock();
    byte[] lockByte = ThemisLock.toByte(expect);
    ThemisLock actual = ThemisLock.parseFromByte(lockByte);
    Assert.assertTrue(expect.equals(actual));
  }
}
