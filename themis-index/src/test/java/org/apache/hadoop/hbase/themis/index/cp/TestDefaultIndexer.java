package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.themis.ThemisDelete;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.ThemisScan;
import org.apache.hadoop.hbase.themis.cache.ColumnMutationCache;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

public class TestDefaultIndexer extends IndexTestBase {
  @Test
  public void testLoadSecondaryIndexesForTable() throws IOException {
    Map<IndexColumn, String> columnIndexes = new HashMap<IndexColumn, String>();
    DefaultIndexer indexer = new DefaultIndexer(conf);

    indexer.loadSecondaryIndexesForTable(admin.getDescriptor(TableName.valueOf(MAIN_TABLE)),
      columnIndexes);
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
    Pair<Cell.Type, byte[]> typeAndValue = mutationCache.getMutation(columnCoordinate);
    Assert.assertNotNull(typeAndValue);
    Assert.assertEquals(Type.Put, typeAndValue.getFirst());
    Assert.assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, typeAndValue.getSecond());
  }

  protected void mockTsAndCommitTransaction() throws IOException {
    mockTimestamp(commitTs);
    transaction.commit();
  }
  
  protected void nextTsAndCreateTransaction() throws IOException {
    nextTransactionTs();
    createTransactionWithMock();    
  }
  
  protected void checkReadResult(byte[] value, ColumnCoordinate column, Result result)
      throws IOException {
    checkReadResults(new byte[][]{value}, new ColumnCoordinate[]{column}, result);
  }
  
  protected void checkReadResults(byte[][] values, ColumnCoordinate[] columns, Result result) {
    Assert.assertEquals(values.length, result.size());
    for (int i = 0; i < values.length; ++i) {
      ColumnCoordinate column = columns[i];
      Assert.assertArrayEquals(values[i], result.getValue(column.getFamily(), column.getQualifier()));
    }
  }

  @Test
  public void testWriteTransactionWithIndex() throws IOException {
    nextTsAndCreateTransaction();
    ThemisPut put = new ThemisPut(ROW);
    put.add(INDEX_FAMILY, INDEX_QUALIFIER, VALUE);
    transaction.put(MAIN_TABLE, put);
    mockTsAndCommitTransaction();
    
    nextTsAndCreateTransaction();
    Result result = transaction.get(MAIN_TABLE, new ThemisGet(ROW).addColumn(
      INDEX_FAMILY, INDEX_QUALIFIER));
    checkReadResult(VALUE, COLUMN, result);
    result = transaction.get(INDEX_TABLE, new ThemisGet(VALUE).addColumn(
      IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES, ROW));
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY,
            CellUtil.cloneValue(result.listCells().get(0)));
    
    nextTsAndCreateTransaction();
    ThemisDelete delete = new ThemisDelete(ROW);
    delete.deleteColumn(INDEX_FAMILY, INDEX_QUALIFIER);
    transaction.delete(MAIN_TABLE, delete);
    mockTimestamp(commitTs);
    transaction.commit();
    
    nextTsAndCreateTransaction();
    result = transaction.get(MAIN_TABLE, new ThemisGet(ROW).addColumn(
      INDEX_FAMILY, INDEX_QUALIFIER));
    Assert.assertTrue(result.isEmpty());
    result = transaction.get(INDEX_TABLE, new ThemisGet(VALUE).addColumn(
      IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES, ROW));
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY,
            CellUtil.cloneValue(result.listCells().get(0)));
  }
  
  @Test
  public void testIndexGet() throws IOException {
    Result result = null;
    nextTsAndCreateTransaction();
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE));
    mockTsAndCommitTransaction();
    
    nextTsAndCreateTransaction();
    
    // no-index scan
    ResultScanner scanner = transaction.getScanner(MAIN_TABLE,
      new ThemisScan().addColumn(FAMILY, QUALIFIER));
    Assert.assertFalse(scanner instanceof IndexScanner);
    checkReadResult(VALUE, COLUMN, scanner.next());
    scanner.close();
    
    // index get column
    IndexGet indexGet = new IndexGet(INDEX_COLUMN, VALUE,
        new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkReadResult(VALUE, COLUMN, scanner.next());
    checkAndCloseScanner(scanner);
    
    // write other qualifier and family
    nextTsAndCreateTransaction();
    transaction.delete(MAIN_TABLE, new ThemisDelete(ROW).deleteColumn(FAMILY, ANOTHER_QUALIFIER));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(ANOTHER_FAMILY, QUALIFIER, ANOTHER_VALUE));
    mockTsAndCommitTransaction();
    
    nextTsAndCreateTransaction();
    // index get family and column
    indexGet = new IndexGet(INDEX_COLUMN, VALUE,
      new DataGet().addColumn(FAMILY, QUALIFIER).addFamily(ANOTHER_FAMILY));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    checkReadResults(new byte[][] { VALUE, ANOTHER_VALUE }, new ColumnCoordinate[] { COLUMN,
        COLUMN_WITH_ANOTHER_FAMILY }, scanner.next());
    checkAndCloseScanner(scanner);
    
    // index get whole row
    indexGet = new IndexGet(INDEX_COLUMN, VALUE, new DataGet());
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    checkReadResults(new byte[][] { VALUE, ANOTHER_VALUE }, new ColumnCoordinate[] { COLUMN,
        COLUMN_WITH_ANOTHER_FAMILY }, scanner.next());
    checkAndCloseScanner(scanner);
    
    // write another value in index column
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(FAMILY, QUALIFIER, ANOTHER_VALUE));
    mockTsAndCommitTransaction();
    
    // also get old value because timestamp is not updated
    indexGet = new IndexGet(INDEX_COLUMN, VALUE, new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkReadResult(VALUE, COLUMN, scanner.next());
    checkAndCloseScanner(scanner);
    
    // can not read the old value
    nextTransactionTs();
    createTransactionWithMock();
    indexGet = new IndexGet(INDEX_COLUMN, VALUE, new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkAndCloseScanner(scanner);
    
    // can read the new value
    nextTsAndCreateTransaction();
    indexGet = new IndexGet(INDEX_COLUMN, ANOTHER_VALUE, new DataGet().addColumn(FAMILY, QUALIFIER)
        .addFamily(ANOTHER_FAMILY));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkReadResults(new byte[][] { ANOTHER_VALUE, ANOTHER_VALUE }, new ColumnCoordinate[] { COLUMN,
        COLUMN_WITH_ANOTHER_FAMILY }, scanner.next());
    checkAndCloseScanner(scanner);
    
    // delete value
    // write another value in index column
    transaction.delete(MAIN_TABLE, new ThemisDelete(ROW).deleteColumn(FAMILY, QUALIFIER));
    mockTsAndCommitTransaction();
    
    // still read old value
    indexGet = new IndexGet(INDEX_COLUMN, ANOTHER_VALUE, new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkReadResult(ANOTHER_VALUE, COLUMN, scanner.next());
    checkAndCloseScanner(scanner);
    
    // can not read value after timestamp updated
    nextTsAndCreateTransaction();
    indexGet = new IndexGet(INDEX_COLUMN, ANOTHER_VALUE, new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkAndCloseScanner(scanner);
    
    // two rows with the same index column value
    nextTsAndCreateTransaction();
    transaction.put(MAIN_TABLE, new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, VALUE));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE));
    mockTsAndCommitTransaction();
    
    nextTsAndCreateTransaction();
    indexGet = new IndexGet(INDEX_COLUMN, VALUE, new DataGet().addColumn(FAMILY, QUALIFIER));
    scanner = transaction.getScanner(MAIN_TABLE, indexGet);
    Assert.assertTrue(scanner instanceof IndexScanner);
    result = scanner.next();
    Assert.assertArrayEquals(ANOTHER_ROW, result.getRow());
    checkReadResult(VALUE, COLUMN, result);
    result = scanner.next();
    Assert.assertArrayEquals(ROW, result.getRow());
    checkReadResult(VALUE, COLUMN, result);
    checkAndCloseScanner(scanner);
  }
  
  @Test
  public void testIndexScan() throws IOException {
    nextTsAndCreateTransaction();
    transaction.put(MAIN_TABLE, new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, ANOTHER_VALUE));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(FAMILY, ANOTHER_QUALIFIER, VALUE));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(ANOTHER_FAMILY, QUALIFIER, VALUE));
    mockTsAndCommitTransaction();
    
    nextTsAndCreateTransaction();
    IndexScan indexScan = new IndexScan(INDEX_COLUMN);
    ResultScanner scanner = transaction.getScanner(MAIN_TABLE, indexScan);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkReadResult(ANOTHER_VALUE, COLUMN, scanner.next());
    checkReadResults(new byte[][] { VALUE, VALUE, VALUE }, new ColumnCoordinate[] { COLUMN,
        COLUMN_WITH_ANOTHER_QUALIFIER, COLUMN_WITH_ANOTHER_FAMILY }, scanner.next());
    checkAndCloseScanner(scanner);
    
    // start/stop row for index scan
    indexScan = new IndexScan(INDEX_COLUMN, ANOTHER_VALUE, ANOTHER_VALUE);
    scanner = transaction.getScanner(MAIN_TABLE, indexScan);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkReadResult(ANOTHER_VALUE, COLUMN, scanner.next());
    checkAndCloseScanner(scanner);
    
    // Delete/Update
    transaction.delete(MAIN_TABLE, new ThemisDelete(ROW).deleteColumn(FAMILY, ANOTHER_QUALIFIER));
    transaction.put(MAIN_TABLE, new ThemisPut(ROW).add(ANOTHER_FAMILY, QUALIFIER, ANOTHER_VALUE));
    mockTsAndCommitTransaction();
    // won't get new result before update timestamp
    indexScan = new IndexScan(INDEX_COLUMN);
    scanner = transaction.getScanner(MAIN_TABLE, indexScan);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkReadResult(ANOTHER_VALUE, COLUMN, scanner.next());
    checkReadResults(new byte[][] { VALUE, VALUE, VALUE }, new ColumnCoordinate[] { COLUMN,
        COLUMN_WITH_ANOTHER_QUALIFIER, COLUMN_WITH_ANOTHER_FAMILY }, scanner.next());
    checkAndCloseScanner(scanner);

    nextTsAndCreateTransaction();
    indexScan = new IndexScan(INDEX_COLUMN);
    scanner = transaction.getScanner(MAIN_TABLE, indexScan);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkReadResult(ANOTHER_VALUE, COLUMN, scanner.next());
    checkReadResults(new byte[][] { VALUE, ANOTHER_VALUE }, new ColumnCoordinate[] { COLUMN,
        COLUMN_WITH_ANOTHER_FAMILY }, scanner.next());
    checkAndCloseScanner(scanner);
    
    // update value to make tow rows have the same index column value
    transaction.put(MAIN_TABLE, new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, VALUE));
    mockTsAndCommitTransaction();
    
    nextTsAndCreateTransaction();
    indexScan = new IndexScan(INDEX_COLUMN);
    scanner = transaction.getScanner(MAIN_TABLE, indexScan);
    Assert.assertTrue(scanner instanceof IndexScanner);
    checkReadResult(VALUE, COLUMN, scanner.next());
    checkReadResults(new byte[][] { VALUE, ANOTHER_VALUE }, new ColumnCoordinate[] { COLUMN,
        COLUMN_WITH_ANOTHER_FAMILY }, scanner.next());
    checkAndCloseScanner(scanner);
  }
}