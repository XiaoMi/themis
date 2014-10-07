package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.ThemisDelete;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.cache.ColumnMutationCache;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

public class TestDefaultIndexer extends IndexTestBase {
  @Test
  public void testLoadSecondaryIndexesForTable() throws IOException {
    createTableForIndexTest();
    Map<IndexColumn, String> columnIndexes = new HashMap<IndexColumn, String>();
    DefaultIndexer indexer = new DefaultIndexer(conf);
    indexer.loadSecondaryIndexesForTable(admin.getTableDescriptor(INDEX_TEST_MAIN_TABLE_NAME),
      columnIndexes);
    deleteTableForIndexTest();
    Assert.assertEquals(1, columnIndexes.size());
    IndexColumn indexColumn = new IndexColumn(INDEX_TEST_MAIN_TABLE_NAME,
        INDEX_TEST_MAIN_TABLE_FAMILY_NAME, INDEX_TEST_MAIN_TABLE_QUALIFIER_NAME);
    Assert.assertTrue(columnIndexes.containsKey(indexColumn));
    Assert.assertEquals(Bytes.toString(INDEX_TEST_INDEX_TABLE_NAME), columnIndexes.get(indexColumn));
  }

  @Test
  public void testAddIndexMutations() throws IOException {
    ColumnMutationCache mutationCache = new ColumnMutationCache();
    mutationCache.addMutation(TABLENAME, KEYVALUE);
    mutationCache.addMutation(INDEX_TEST_MAIN_TABLE_NAME, new KeyValue(ROW, FAMILY,
        INDEX_TEST_MAIN_TABLE_QUALIFIER_NAME, Long.MAX_VALUE, Type.Put, VALUE));
    mutationCache.addMutation(INDEX_TEST_MAIN_TABLE_NAME, new KeyValue(ROW,
        INDEX_TEST_MAIN_TABLE_FAMILY_NAME, QUALIFIER, Long.MAX_VALUE, Type.Put, VALUE));
    mutationCache.addMutation(INDEX_TEST_MAIN_TABLE_NAME, new KeyValue(ROW,
        INDEX_TEST_MAIN_TABLE_FAMILY_NAME, INDEX_TEST_MAIN_TABLE_QUALIFIER_NAME, Long.MAX_VALUE,
        Type.DeleteColumn, VALUE));
    createTableForIndexTest();
    DefaultIndexer indexer = new DefaultIndexer(conf);
    indexer.addIndexMutations(mutationCache);
    Assert.assertEquals(4, mutationCache.size());

    mutationCache.addMutation(INDEX_TEST_MAIN_TABLE_NAME, new KeyValue(ROW,
        INDEX_TEST_MAIN_TABLE_FAMILY_NAME, INDEX_TEST_MAIN_TABLE_QUALIFIER_NAME, Long.MAX_VALUE,
        Type.Put, VALUE));
    indexer.addIndexMutations(mutationCache);
    Assert.assertEquals(5, mutationCache.size());
    ColumnCoordinate columnCoordinate = new ColumnCoordinate(INDEX_TEST_INDEX_TABLE_NAME, VALUE,
        IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES, ROW);
    Pair<Type, byte[]> typeAndValue = mutationCache.getMutation(columnCoordinate);
    Assert.assertNotNull(typeAndValue);
    Assert.assertEquals(Type.Put, typeAndValue.getFirst());
    Assert.assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, typeAndValue.getSecond());
    deleteTableForIndexTest();
  }

  @Test
  public void testTransactionWithIndex() throws IOException {
    createTableForIndexTest();
    conf.set(TransactionConstant.INDEXER_CLASS_KEY, DefaultIndexer.class.getName());
    createTransactionWithMock();
    ThemisPut put = new ThemisPut(ROW);
    put.add(INDEX_TEST_MAIN_TABLE_FAMILY_NAME, INDEX_TEST_MAIN_TABLE_QUALIFIER_NAME, VALUE);
    transaction.put(INDEX_TEST_MAIN_TABLE_NAME, put);
    mockTimestamp(commitTs);
    transaction.commit();
    nextTransactionTs();
    createTransactionWithMock();
    Result result = transaction.get(INDEX_TEST_MAIN_TABLE_NAME, new ThemisGet(ROW).addColumn(
      INDEX_TEST_MAIN_TABLE_FAMILY_NAME, INDEX_TEST_MAIN_TABLE_QUALIFIER_NAME));
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(VALUE, result.list().get(0).getValue());
    result = transaction.get(INDEX_TEST_INDEX_TABLE_NAME, new ThemisGet(VALUE).addColumn(
      IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES, ROW));
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, result.list().get(0).getValue());
    
    nextTransactionTs();
    createTransactionWithMock();
    ThemisDelete delete = new ThemisDelete(ROW);
    delete.deleteColumn(INDEX_TEST_MAIN_TABLE_FAMILY_NAME, INDEX_TEST_MAIN_TABLE_QUALIFIER_NAME);
    transaction.delete(INDEX_TEST_MAIN_TABLE_NAME, delete);
    mockTimestamp(commitTs);
    transaction.commit();
    
    nextTransactionTs();
    createTransactionWithMock();
    result = transaction.get(INDEX_TEST_MAIN_TABLE_NAME, new ThemisGet(ROW).addColumn(
      INDEX_TEST_MAIN_TABLE_FAMILY_NAME, INDEX_TEST_MAIN_TABLE_QUALIFIER_NAME));
    Assert.assertTrue(result.isEmpty());
    result = transaction.get(INDEX_TEST_INDEX_TABLE_NAME, new ThemisGet(VALUE).addColumn(
      IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES, ROW));
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, result.list().get(0).getValue());
    deleteTableForIndexTest();
  }
}