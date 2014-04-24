package org.apache.hadoop.hbase.themis.columns;

import junit.framework.Assert;

import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.junit.Test;

public class TestColumnCoordinate extends TestBase {
  @Test
  public void testHashCode() throws Exception {
    Assert.assertTrue(COLUMN.hashCode() != COLUMN_WITH_ANOTHER_TABLE.hashCode());
    Assert.assertTrue(COLUMN.hashCode() != COLUMN_WITH_ANOTHER_ROW.hashCode());
    Assert.assertTrue(COLUMN.hashCode() != COLUMN_WITH_ANOTHER_FAMILY.hashCode());
    Assert.assertTrue(COLUMN.hashCode() != COLUMN_WITH_ANOTHER_QUALIFIER.hashCode());
  }
  
  @Test
  public void testWriteAndReadColumn() throws Exception {
    ColumnCoordinate actual = new ColumnCoordinate();
    writeObjectToBufferAndRead(COLUMN, actual);
    Assert.assertTrue(COLUMN.equals(actual));
  }
}
