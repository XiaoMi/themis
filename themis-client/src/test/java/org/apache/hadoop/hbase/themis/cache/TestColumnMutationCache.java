package org.apache.hadoop.hbase.themis.cache;

import junit.framework.Assert;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.cache.ColumnMutationCache;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestColumnMutationCache extends TestBase {
  @Test
  public void testAddAndGetMutationsForRow() {
    ColumnMutationCache cache = new ColumnMutationCache();
    Assert.assertTrue(cache.addMutation(TABLENAME, KEYVALUE));
    Assert.assertEquals(1, cache.size());
    Assert.assertTrue(cache.hasMutation(COLUMN));
    Assert.assertFalse(cache.addMutation(TABLENAME, KEYVALUE));
    Assert.assertEquals(1, cache.size());
    
    KeyValue kvWithAnotherValue = new KeyValue(ROW, FAMILY, QUALIFIER, PREWRITE_TS, Type.Put, ANOTHER_VALUE);
    Assert.assertFalse(cache.addMutation(TABLENAME, kvWithAnotherValue));
    Assert.assertEquals(1, cache.size());
    byte[] actualValue = cache.getMutation(COLUMN).getSecond();
    System.out.println(Bytes.toString(actualValue));
    Assert.assertTrue(Bytes.equals(ANOTHER_VALUE, actualValue));
    Assert.assertTrue(cache.hasMutation(COLUMN));
    
    Assert.assertTrue(cache.addMutation(ANOTHER_TABLENAME, KEYVALUE));
    Assert.assertEquals(2, cache.size());
    Assert.assertTrue(cache.hasMutation(COLUMN));
    Assert.assertTrue(cache.hasMutation(COLUMN_WITH_ANOTHER_TABLE));
    
    KeyValue deleteKv = ClientTestBase.getKeyValue(COLUMN, Type.DeleteColumn, PREWRITE_TS);
    Assert.assertFalse(cache.addMutation(TABLENAME, deleteKv));
    Assert.assertEquals(2, cache.size());
    Assert.assertEquals(Type.DeleteColumn, cache.getMutation(COLUMN).getFirst());
  }  
}
