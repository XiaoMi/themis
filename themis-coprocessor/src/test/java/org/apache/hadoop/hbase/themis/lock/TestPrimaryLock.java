package org.apache.hadoop.hbase.themis.lock;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell.Type;
import org.junit.Test;

public class TestPrimaryLock extends TestThemisLock {
  @Test
  public void testAddSecondaryColumn() throws IOException {
    PrimaryLock lock = new PrimaryLock();
    lock.addSecondaryColumn(COLUMN, Type.Put);
    assertEquals(Type.Put, lock.getSecondaryColumn(COLUMN));
    lock.addSecondaryColumn(COLUMN, Type.DeleteColumn);
    assertEquals(Type.DeleteColumn, lock.getSecondaryColumn(COLUMN));
    lock.addSecondaryColumn(COLUMN_WITH_ANOTHER_TABLE, Type.Put);
    assertEquals(2, lock.getSecondaryColumns().size());
  }

  @Test
  public void testWriteAndReadPrimaryLock() throws IOException {
    PrimaryLock expect = getPrimaryLock();
    ThemisLock actual = new PrimaryLock();
    writeObjectToBufferAndRead(expect, actual);
    assertEquals(expect, actual);
  }

  @Test
  public void testToByteAndParseFromByte() throws IOException {
    PrimaryLock expect = getPrimaryLock();
    byte[] lockByte = ThemisLock.toByte(expect);
    ThemisLock actual = ThemisLock.parseFromByte(lockByte);
    assertEquals(expect, actual);
  }
}
