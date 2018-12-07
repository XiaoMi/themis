package org.apache.hadoop.hbase.themis.columns;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.themis.TestBase;
import org.junit.Test;

public class TestColumnCoordinate extends TestBase {

  @Test
  public void testHashCode() throws Exception {
    assertTrue(COLUMN.hashCode() != COLUMN_WITH_ANOTHER_TABLE.hashCode());
    assertTrue(COLUMN.hashCode() != COLUMN_WITH_ANOTHER_ROW.hashCode());
    assertTrue(COLUMN.hashCode() != COLUMN_WITH_ANOTHER_FAMILY.hashCode());
    assertTrue(COLUMN.hashCode() != COLUMN_WITH_ANOTHER_QUALIFIER.hashCode());
  }

  @Test
  public void testWriteAndReadColumn() throws Exception {
    ColumnCoordinate actual = new ColumnCoordinate();
    writeObjectToBufferAndRead(COLUMN, actual);
    assertTrue(COLUMN.equals(actual));
  }
}
