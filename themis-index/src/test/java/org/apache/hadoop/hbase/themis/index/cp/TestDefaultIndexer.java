package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.themis.ThemisDelete;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.ThemisScan;
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
    indexer.loadSecondaryIndexesForTable(admin.getTableDescriptor(MAIN_TABLE),
      columnIndexes);
    deleteTableForIndexTest();
    Assert.assertEquals(1, columnIndexes.size());
    IndexColumn indexColumn = new IndexColumn(MAIN_TABLE,
        INDEX_FAMILY, INDEX_QUALIFIER);
    Assert.assertTrue(columnIndexes.containsKey(indexColumn));
    Assert.assertEquals(Bytes.toString(INDEX_TABLE), columnIndexes.get(indexColumn));
  }

  @Test
  public void testAddIndexMutations() throws IOException {
    ColumnMutationCache mutationCache = new ColumnMutationCache();
    mutationCache.addMutation(TABLENAME, KEYVALUE);
    mutationCache.addMutation(MAIN_TABLE, new KeyValue(ROW, ANOTHER_FAMILY,
        INDEX_QUALIFIER, Long.MAX_VALUE, Type.Put, VALUE));
    mutationCache.addMutation(MAIN_TABLE, new KeyValue(ROW,
        INDEX_FAMILY, ANOTHER_QUALIFIER, Long.MAX_VALUE, Type.Put, VALUE));
    mutationCache.addMutation(MAIN_TABLE, new KeyValue(ROW,
        INDEX_FAMILY, INDEX_QUALIFIER, Long.MAX_VALUE,
        Type.DeleteColumn, VALUE));
    createTableForIndexTest();
    DefaultIndexer indexer = new DefaultIndexer(conf);
    indexer.addIndexMutations(mutationCache);
    Assert.assertEquals(4, mutationCache.size());

    mutationCache.addMutation(MAIN_TABLE, new KeyValue(ROW,
        INDEX_FAMILY, INDEX_QUALIFIER, Long.MAX_VALUE,
        Type.Put, VALUE));
    indexer.addIndexMutations(mutationCache);
    Assert.assertEquals(5, mutationCache.size());
    ColumnCoordinate columnCoordinate = new ColumnCoordinate(INDEX_TABLE, VALUE,
        IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES, ROW);
    Pair<Type, byte[]> typeAndValue = mutationCache.getMutation(columnCoordinate);
    Assert.assertNotNull(typeAndValue);
    Assert.assertEquals(Type.Put, typeAndValue.getFirst());
    Assert.assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, typeAndValue.getSecond());
    deleteTableForIndexTest();
  }

  @Test
  public void testWriteTransactionWithIndex() throws IOException {
    createTableForIndexTest();
    conf.set(TransactionConstant.INDEXER_CLASS_KEY, DefaultIndexer.class.getName());
    createTransactionWithMock();
    ThemisPut put = new ThemisPut(ROW);
    put.add(INDEX_FAMILY, INDEX_QUALIFIER, VALUE);
    transaction.put(MAIN_TABLE, put);
    mockTimestamp(commitTs);
    transaction.commit();
    nextTransactionTs();
    createTransactionWithMock();
    Result result = transaction.get(MAIN_TABLE, new ThemisGet(ROW).addColumn(
      INDEX_FAMILY, INDEX_QUALIFIER));
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(VALUE, result.list().get(0).getValue());
    result = transaction.get(INDEX_TABLE, new ThemisGet(VALUE).addColumn(
      IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES, ROW));
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, result.list().get(0).getValue());
    
    nextTransactionTs();
    createTransactionWithMock();
    ThemisDelete delete = new ThemisDelete(ROW);
    delete.deleteColumn(INDEX_FAMILY, INDEX_QUALIFIER);
    transaction.delete(MAIN_TABLE, delete);
    mockTimestamp(commitTs);
    transaction.commit();
    
    nextTransactionTs();
    createTransactionWithMock();
    result = transaction.get(MAIN_TABLE, new ThemisGet(ROW).addColumn(
      INDEX_FAMILY, INDEX_QUALIFIER));
    Assert.assertTrue(result.isEmpty());
    result = transaction.get(INDEX_TABLE, new ThemisGet(VALUE).addColumn(
      IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES, ROW));
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, result.list().get(0).getValue());
    deleteTableForIndexTest();
  }
  
  protected void mockTsAndCommitTransaction() throws IOException {
    mockTimestamp(commitTs);
    transaction.commit();
  }
  
  @Test
  public void testIndexGet() throws IOException {
    createTableForIndexTest();
    conf.set(TransactionConstant.INDEXER_CLASS_KEY, DefaultIndexer.class.getName());
    createTransactionWithMock();
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE));
    mockTsAndCommitTransaction();
    
    nextTransactionTs();
    createTransactionWithMock();
    
    // no-index scan
    ResultScanner scanner = transaction.getScanner(MAIN_TABLE,
      new ThemisScan().addColumn(FAMILY, QUALIFIER));
    Result result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // index get column
    IndexGet indexGet = new IndexGet(INDEX_COLUMN, VALUE,
        new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // write other qualifier and family
    nextTransactionTs();
    createTransactionWithMock();
    transaction.delete(MAIN_TABLE, new ThemisDelete(ROW).deleteColumn(FAMILY, ANOTHER_QUALIFIER));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(ANOTHER_FAMILY, QUALIFIER, ANOTHER_VALUE));
    mockTsAndCommitTransaction();
    
    nextTransactionTs();
    createTransactionWithMock();
    // index get family and column
    indexGet = new IndexGet(INDEX_COLUMN, VALUE,
      new DataGet().addColumn(FAMILY, QUALIFIER).addFamily(ANOTHER_FAMILY));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    result = scanner.next();
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // index get whole row
    indexGet = new IndexGet(INDEX_COLUMN, VALUE, new DataGet());
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    result = scanner.next();
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // write another value in index column
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(FAMILY, QUALIFIER, ANOTHER_VALUE));
    mockTsAndCommitTransaction();
    
    // also get old value because timestamp is not updated
    indexGet = new IndexGet(INDEX_COLUMN, VALUE, new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // can not read the old value
    nextTransactionTs();
    createTransactionWithMock();
    indexGet = new IndexGet(INDEX_COLUMN, VALUE, new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // can read the new value
    nextTransactionTs();
    createTransactionWithMock();
    indexGet = new IndexGet(INDEX_COLUMN, ANOTHER_VALUE, new DataGet().addColumn(FAMILY, QUALIFIER)
        .addFamily(ANOTHER_FAMILY));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    result = scanner.next();
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // delete value
    // write another value in index column
    transaction.delete(MAIN_TABLE, new ThemisDelete(ROW).deleteColumn(FAMILY, QUALIFIER));
    mockTsAndCommitTransaction();
    
    // still read old value
    indexGet = new IndexGet(INDEX_COLUMN, ANOTHER_VALUE, new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // can not read value after timestamp updated
    nextTransactionTs();
    createTransactionWithMock();
    indexGet = new IndexGet(INDEX_COLUMN, ANOTHER_VALUE, new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // two rows with the same index column value
    nextTransactionTs();
    createTransactionWithMock();
    transaction.put(MAIN_TABLE, new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, VALUE));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE));
    mockTsAndCommitTransaction();
    
    nextTransactionTs();
    createTransactionWithMock();
    indexGet = new IndexGet(INDEX_COLUMN, VALUE, new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(ANOTHER_ROW, result.getRow());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(ROW, result.getRow());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    deleteTableForIndexTest();
  }
  
  @Test
  public void testIndexScan() throws IOException {
    createTableForIndexTest();
    
    conf.set(TransactionConstant.INDEXER_CLASS_KEY, DefaultIndexer.class.getName());
    createTransactionWithMock();
    transaction.put(MAIN_TABLE, new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, ANOTHER_VALUE));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(FAMILY, ANOTHER_QUALIFIER, VALUE));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(ANOTHER_FAMILY, QUALIFIER, VALUE));
    mockTsAndCommitTransaction();
    
    nextTransactionTs();
    createTransactionWithMock();
    IndexScan indexScan = new IndexScan(INDEX_COLUMN);
    ResultScanner scanner = transaction.getScanner(MAIN_TABLE, indexScan);
    Assert.assertTrue(scanner instanceof IndexScanner);
    Result result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(FAMILY, QUALIFIER));
    result = scanner.next();
    Assert.assertEquals(3, result.size());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, ANOTHER_QUALIFIER));
    Assert.assertArrayEquals(VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // start/stop row for index scan
    indexScan = new IndexScan(INDEX_COLUMN, ANOTHER_VALUE, ANOTHER_VALUE);
    scanner = transaction.getScanner(MAIN_TABLE, indexScan);
    Assert.assertTrue(scanner instanceof IndexScanner);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // Delete/Update
    transaction.delete(MAIN_TABLE, new ThemisDelete(ROW).deleteColumn(FAMILY, ANOTHER_QUALIFIER));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(ANOTHER_FAMILY, QUALIFIER, ANOTHER_VALUE));
    mockTsAndCommitTransaction();
    // won't get new result before update timestamp
    indexScan = new IndexScan(INDEX_COLUMN);
    scanner = transaction.getScanner(MAIN_TABLE, indexScan);
    Assert.assertTrue(scanner instanceof IndexScanner);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(FAMILY, QUALIFIER));
    result = scanner.next();
    Assert.assertEquals(3, result.size());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, ANOTHER_QUALIFIER));
    Assert.assertArrayEquals(VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    
    nextTransactionTs();
    createTransactionWithMock();
    indexScan = new IndexScan(INDEX_COLUMN);
    scanner = transaction.getScanner(MAIN_TABLE, indexScan);
    Assert.assertTrue(scanner instanceof IndexScanner);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(FAMILY, QUALIFIER));
    result = scanner.next();
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    // update value to make tow rows have the same index column value
    transaction.put(MAIN_TABLE, new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, VALUE));
    mockTsAndCommitTransaction();
    
    nextTransactionTs();
    createTransactionWithMock();
    indexScan = new IndexScan(INDEX_COLUMN);
    scanner = transaction.getScanner(MAIN_TABLE, indexScan);
    Assert.assertTrue(scanner instanceof IndexScanner);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    result = scanner.next();
    Assert.assertEquals(2, result.size());
    Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    Assert.assertArrayEquals(ANOTHER_VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER));
    Assert.assertNull(scanner.next());
    scanner.close();
    
    deleteTableForIndexTest();
  }
}