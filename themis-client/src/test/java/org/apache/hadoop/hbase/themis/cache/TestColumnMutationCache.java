package org.apache.hadoop.hbase.themis.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

public class TestColumnMutationCache extends TestBase {

  @Test
  public void testAddAndGetMutationsForRow() {
    ColumnMutationCache cache = new ColumnMutationCache();
    assertTrue(cache.addMutation(TABLENAME, KEYVALUE));
    assertEquals(1, cache.size());
    assertTrue(cache.hasMutation(COLUMN));
    assertFalse(cache.addMutation(TABLENAME, KEYVALUE));
    assertEquals(1, cache.size());

    Cell kvWithAnotherValue = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW)
      .setFamily(FAMILY).setQualifier(QUALIFIER).setTimestamp(PREWRITE_TS).setType(Type.Put)
      .setValue(ANOTHER_VALUE).build();
    assertFalse(cache.addMutation(TABLENAME, kvWithAnotherValue));
    assertEquals(1, cache.size());
    byte[] actualValue = cache.getMutation(COLUMN).getSecond();
    assertTrue(Bytes.equals(ANOTHER_VALUE, actualValue));
    assertTrue(cache.hasMutation(COLUMN));

    assertTrue(cache.addMutation(ANOTHER_TABLENAME, KEYVALUE));
    assertEquals(2, cache.size());
    assertTrue(cache.hasMutation(COLUMN));
    assertTrue(cache.hasMutation(COLUMN_WITH_ANOTHER_TABLE));

    Cell deleteKv = ClientTestBase.getKeyValue(COLUMN, Type.DeleteColumn, PREWRITE_TS);
    assertFalse(cache.addMutation(TABLENAME, deleteKv));
    assertEquals(2, cache.size());
    assertEquals(Type.DeleteColumn, cache.getMutation(COLUMN).getFirst());

    cache.addMutation(TABLENAME,
      ClientTestBase.getKeyValue(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS));
    Pair<Integer, Integer> mutationsCount = cache.getMutationsCount();
    assertEquals(2, mutationsCount.getFirst().intValue());
    assertEquals(3, mutationsCount.getSecond().intValue());
  }
}
