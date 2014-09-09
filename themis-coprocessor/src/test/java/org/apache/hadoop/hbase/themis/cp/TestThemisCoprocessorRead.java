package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._;
import org.junit.Assert;
import org.junit.Test;

public class TestThemisCoprocessorRead extends TransactionTestBase {
  protected ColumnCoordinate[] columnsWithPutAndDeletePut = new ColumnCoordinate[] { COLUMN,
      COLUMN_WITH_ANOTHER_ROW };
  
  protected void checkGetsByWriteColumnsKvs(Get get, byte[] row, byte[] family, byte[] qualifier,
      long timestamp) throws IOException {
    Assert.assertTrue(Bytes.equals(row, get.getRow()));
    Assert.assertEquals(1, get.getFamilyMap().size());
    Assert.assertEquals(1, get.getFamilyMap().get(family).size());
    Assert.assertTrue(get.getFamilyMap().get(family).contains(qualifier));
    Assert.assertEquals(timestamp, get.getTimeRange().getMin());
    Assert.assertEquals(timestamp + 1, get.getTimeRange().getMax());
  }
  
  protected void checkGetOneColumnResult(ColumnCoordinate columnCoordinate, Result internalResult)
      throws IOException {
    Assert.assertFalse(ThemisCpUtil.isLockResult(internalResult));
    if (getColumnType(columnCoordinate).equals(Type.Put)) {
      Assert.assertEquals(1, internalResult.size());
      KeyValue kv = internalResult.list().get(0);
      Assert.assertTrue(columnCoordinate.equals(new ColumnCoordinate(columnCoordinate.getTableName(), kv.getRow(),
        kv.getFamily(), kv.getQualifier())));
      Assert.assertEquals(prewriteTs, kv.getTimestamp());
      Assert.assertArrayEquals(VALUE, kv.getValue());
    } else {
      Assert.assertEquals(0, internalResult.size());
    }
  }
  
  @Test
  public void testThemisGetSuccess() throws IOException {
    commitTestTransaction();
    // test get one column
    for (ColumnCoordinate columnCoordinate : TRANSACTION_COLUMNS) {
      Get get = new Get(columnCoordinate.getRow()).addColumn(columnCoordinate.getFamily(), columnCoordinate.getQualifier());
      Result iResult = cpClient.themisGet(columnCoordinate.getTableName(), get, commitTs + 1);
      checkGetOneColumnResult(columnCoordinate, iResult);
    }
    
    // test get one row
    Get get = new Get(COLUMN.getRow());
    get.addColumn(FAMILY, QUALIFIER);
    get.addColumn(FAMILY, ANOTHER_QUALIFIER); // the type of the column is 'DeleteColumn'
    get.addColumn(ANOTHER_FAMILY, QUALIFIER);
    Result iResult = cpClient.themisGet(TABLENAME, get, commitTs + 1);
    Assert.assertFalse(ThemisCpUtil.isLockResult(iResult));
    Assert.assertEquals(2, iResult.size());
    for (KeyValue kv : iResult.list()) {
      Assert.assertEquals(prewriteTs, kv.getTimestamp());
    }
  }
  
  @Test
  public void testThemisGetWithUnknowColumn() throws IOException {
    commitTestTransaction();
    // unknown family
    Get get = new Get(COLUMN.getRow());
    get.addColumn(FAMILY, QUALIFIER);
    byte[] NULL_FAMILY = Bytes.toBytes("Null" + Bytes.toString(FAMILY));
    get.addColumn(NULL_FAMILY, QUALIFIER);
    try {
      cpClient.themisGet(TABLENAME, get, commitTs + 1);
      Assert.fail();
    } catch (Exception e) {}
    // unknown qualifier
    get = new Get(COLUMN.getRow());
    get.addColumn(FAMILY, QUALIFIER);
    byte[] NULL_QUALIFIER = Bytes.toBytes("Null" + Bytes.toString(QUALIFIER));
    get.addColumn(FAMILY, NULL_QUALIFIER);
    Result iResult = cpClient.themisGet(TABLENAME, get, commitTs + 1);
    Assert.assertFalse(ThemisCpUtil.isLockResult(iResult));
    Assert.assertEquals(1, iResult.size());
    Assert.assertEquals(prewriteTs, iResult.list().get(0).getTimestamp());
  }
 
  protected void commitColumnsWithDifferentTs() throws IOException {
    commitOneColumn(COLUMN, Type.Put);
    commitOneColumn(COLUMN_WITH_ANOTHER_FAMILY, Type.Put);
    nextTransactionTs();
    commitOneColumn(COLUMN_WITH_ANOTHER_FAMILY, Type.Put);
    commitOneColumn(COLUMN_WITH_ANOTHER_QUALIFIER, Type.Put);
    nextTransactionTs();
  }
  
  protected void checkGetResultForDifferentTs(Result iResult) throws IOException {
    Assert.assertFalse(ThemisCpUtil.isLockResult(iResult));
    Assert.assertEquals(3, iResult.list().size());
    Collections.sort(iResult.list(), new KVComparator());
    Result result = new Result(iResult.list());
    Assert.assertEquals(1, result.getColumn(COLUMN.getFamily(), COLUMN.getQualifier()).size());
    Assert.assertEquals(prewriteTs - 200,
      result.getColumn(COLUMN.getFamily(), COLUMN.getQualifier()).get(0).getTimestamp());
    for (ColumnCoordinate columnCoordinate : new ColumnCoordinate[] {COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER}) {
      Assert.assertEquals(1, result.getColumn(columnCoordinate.getFamily(), columnCoordinate.getQualifier()).size());
      Assert.assertEquals(prewriteTs - 100,
        result.getColumn(columnCoordinate.getFamily(), columnCoordinate.getQualifier()).get(0).getTimestamp());
    }    
  }
  
  protected Get createGetForDifferentTs() {
    Get get = new Get(ROW);
    get.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
    get.addColumn(COLUMN_WITH_ANOTHER_FAMILY.getFamily(), COLUMN_WITH_ANOTHER_FAMILY.getQualifier());
    get.addColumn(COLUMN_WITH_ANOTHER_QUALIFIER.getFamily(), COLUMN_WITH_ANOTHER_QUALIFIER.getQualifier());
    return get;
  }
  
  @Test
  public void testGetForColumnsWithDifferentTs() throws IOException {
    commitColumnsWithDifferentTs();
    Result iResult = cpClient.themisGet(TABLENAME, createGetForDifferentTs(), prewriteTs);
    checkGetResultForDifferentTs(iResult);
  }
  
  protected void checkGetOneColumnConflictResult(ColumnCoordinate columnCoordinate, Result internalResult)
      throws IOException {
    Assert.assertTrue(ThemisCpUtil.isLockResult(internalResult));
    Assert.assertEquals(1, internalResult.list().size());
    KeyValue kv = internalResult.list().get(0);
    Assert.assertTrue(ColumnUtil.getLockColumn(columnCoordinate).equals(
      new ColumnCoordinate(columnCoordinate.getTableName(), kv.getRow(), kv.getFamily(), kv.getQualifier())));
    Assert.assertEquals(prewriteTs, kv.getTimestamp());
    Assert.assertArrayEquals(ThemisLock.toByte(getLock(columnCoordinate)), kv.getValue());
  }
  
  @Test
  public void testThemisGetWithOneColumnLockConflict() throws IOException {
    nextTransactionTs();
    // test one column
    for (ColumnCoordinate columnCoordinate : TRANSACTION_COLUMNS) {
      writeLockAndData(columnCoordinate);
      Get get = new Get(columnCoordinate.getRow()).addColumn(columnCoordinate.getFamily(), columnCoordinate.getQualifier());
      // will encounter lock conflict
      Result iResult = cpClient.themisGet(columnCoordinate.getTableName(), get, commitTs);
      checkGetOneColumnConflictResult(columnCoordinate, iResult);
      // newer lock, won't encounter lock conflict
      iResult = cpClient.themisGet(columnCoordinate.getTableName(), get, prewriteTs);
      Assert.assertFalse(ThemisCpUtil.isLockResult(iResult));
      Assert.assertTrue(iResult.isEmpty());
    }
  }
  
  @Test
  public void testThemisGetWithRowLockConflict() throws IOException {
    // test one row
    // a column with conflict lock
    deleteOldDataAndUpdateTs();
    commitTestTransaction();
    nextTransactionTs();
    writeLockAndData(COLUMN);
    Get get = new Get(COLUMN.getRow());
    get.addColumn(FAMILY, QUALIFIER);
    get.addColumn(ANOTHER_FAMILY, QUALIFIER);
    get.addColumn(FAMILY, ANOTHER_QUALIFIER); // the type of the column is 'DeleteColumn'
    Result iResult = cpClient.themisGet(TABLENAME, get, commitTs);
    checkGetOneColumnConflictResult(COLUMN, iResult);
    // a column with conflict lock but will ignore lock
    iResult = cpClient.themisGet(TABLENAME, get, commitTs, true);
    Assert.assertFalse(ThemisCpUtil.isLockResult(iResult));
    Assert.assertEquals(2, iResult.list().size());
    // a column with newer lock, won't cause lock conflict
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertFalse(ThemisCpUtil.isLockResult(iResult));
    Assert.assertEquals(2, iResult.list().size());
  }

  @Test
  public void testGetOneCellFromMultiColumnsRow() throws IOException {
    commitOneColumn(new ColumnCoordinate(TABLENAME, ROW, ANOTHER_FAMILY, ANOTHER_QUALIFIER), Type.Put);
    commitOneColumn(COLUMN_WITH_ANOTHER_FAMILY, Type.Put);
    commitOneColumn(COLUMN_WITH_ANOTHER_QUALIFIER, Type.Put);
    commitOneColumn(COLUMN, Type.Put);
    nextTransactionTs();
    Get get = new Get(ROW);
    get.addColumn(COLUMN_WITH_ANOTHER_QUALIFIER.getFamily(),
      COLUMN_WITH_ANOTHER_QUALIFIER.getQualifier());
    Result result = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertEquals(1, result.list().size());
    get = new Get(ROW);
    get.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
    result = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertEquals(1, result.list().size());
    get = new Get(ROW);
    get.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
    get.addColumn(COLUMN_WITH_ANOTHER_QUALIFIER.getFamily(),
      COLUMN_WITH_ANOTHER_QUALIFIER.getQualifier());
    result = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertEquals(2, result.list().size());
  }

  @Test
  public void testColumnTimestampFilterInGet() throws IOException {
    commitOneColumn(COLUMN, Type.Put, prewriteTs, commitTs);
    // ColumnTimestampFilter should filter this cell
    commitOneColumn(COLUMN, Type.Put, prewriteTs + 20, prewriteTs + 50);
    commitOneColumn(COLUMN_WITH_ANOTHER_FAMILY, Type.Put, prewriteTs + 30, prewriteTs + 40);
    Get get = new Get(ROW);
    get.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
    get.addColumn(COLUMN_WITH_ANOTHER_FAMILY.getFamily(), COLUMN_WITH_ANOTHER_FAMILY.getQualifier());
    Result iResult = cpClient.themisGet(TABLENAME, get, prewriteTs + 50);
    Assert.assertFalse(ThemisCpUtil.isLockResult(iResult));
    Assert.assertEquals(2, iResult.list().size());
    Collections.sort(iResult.list(), new KVComparator());
    Result result = new Result(iResult.list());
    Assert.assertEquals(prewriteTs,
      result.getColumnLatest(COLUMN.getFamily(), COLUMN.getQualifier()).getTimestamp());
    Assert.assertEquals(prewriteTs + 30,
      result.getColumnLatest(COLUMN_WITH_ANOTHER_FAMILY.getFamily(),
        COLUMN_WITH_ANOTHER_FAMILY.getQualifier()).getTimestamp());
  }
  
  @Test
  public void testGetWithRowkeyFilter() throws IOException {
    commitColumnsWithDifferentTs();
    Get get = createGetForDifferentTs();
    get.setFilter(new PrefixFilter(ROW));
    Result iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);
    get.setFilter(new PrefixFilter(ANOTHER_ROW));
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertTrue(iResult.isEmpty());
    FilterList filterList = new FilterList();
    filterList.addFilter(new PrefixFilter(ROW));
    filterList.addFilter(new PrefixFilter(ANOTHER_ROW));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertTrue(iResult.isEmpty());
    filterList = new FilterList(Operator.MUST_PASS_ONE);
    filterList.addFilter(new PrefixFilter(ROW));
    filterList.addFilter(new PrefixFilter(ANOTHER_ROW));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);
  }
  
  @Test
  public void testGetWithNoRowkeyFilter() throws IOException {
    // test ValueFilter
    commitColumnsWithDifferentTs();
    Get get = createGetForDifferentTs();
    get.setFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    Result iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertTrue(iResult.isEmpty());
    // test SingleColumnValueFilter
    get.setFilter(new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOp.EQUAL, ANOTHER_VALUE));
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertTrue(iResult.isEmpty());
    
    writeData(COLUMN, prewriteTs - 200, ANOTHER_VALUE);
    writePutColumn(COLUMN, prewriteTs - 200, commitTs - 200);
    get.setFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertTrue(iResult.list().size() == 1 && !ThemisCpUtil.isLockResult(iResult));
    Assert.assertEquals(prewriteTs - 200, iResult.list().get(0).getTimestamp());
    
    get.setFilter(new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOp.EQUAL, ANOTHER_VALUE));
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);
    
    // test FilterList with MUST_PASS_ALL
    FilterList filterList = new FilterList();
    filterList.addFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    filterList.addFilter(new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOp.EQUAL, ANOTHER_VALUE));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertTrue(iResult.list().size() == 1 && !ThemisCpUtil.isLockResult(iResult));
    Assert.assertEquals(prewriteTs - 200, iResult.list().get(0).getTimestamp());
    
    // test FilterList with MUST_PASS_ONE
    filterList = new FilterList(Operator.MUST_PASS_ONE);
    filterList.addFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    filterList.addFilter(new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOp.EQUAL, ANOTHER_VALUE));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);
    
    // test transfer filter
    get.setFilter(new KeyOnlyFilter());
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);
    for (KeyValue kv : iResult.list()) {
      Assert.assertEquals(0, kv.getValueLength());
    }
  }
  
  @Test
  public void testGetWithFilter() throws IOException {
    // rowkey filter pass while column filter not pass
    commitColumnsWithDifferentTs();
    Get get = createGetForDifferentTs();
    FilterList filterList = new FilterList();
    filterList.addFilter(new PrefixFilter(ROW));
    filterList.addFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    get.setFilter(filterList);
    Result iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertTrue(iResult.isEmpty());
    
    filterList = new FilterList(Operator.MUST_PASS_ONE);
    filterList.addFilter(new PrefixFilter(ROW));
    filterList.addFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);
    
    // rowkey filter not pass while column filter pass
    filterList = new FilterList();
    filterList.addFilter(new PrefixFilter(ANOTHER_ROW));
    filterList.addFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(VALUE)));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    Assert.assertTrue(iResult.isEmpty());
    
    filterList = new FilterList(Operator.MUST_PASS_ONE);
    filterList.addFilter(new PrefixFilter(ANOTHER_ROW));
    filterList.addFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(VALUE)));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);
  }
  
  @Test
  public void testGetLockAndErase() throws IOException {
    // test without lock
    Assert.assertNull(cpClient.getLockAndErase(COLUMN, prewriteTs));
    // test with lock
    nextTransactionTs();
    writeLockAndData(COLUMN);
    ThemisLock lock = cpClient.getLockAndErase(COLUMN, prewriteTs);
    Assert.assertTrue(lock.equals(getLock(COLUMN)));
    Assert.assertNull(readLockBytes(COLUMN));
  }
  
  @Test
  public void testWriteNonThemisFamily() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    byte[] testTable = Bytes.toBytes("test_table");
    byte[] testFamily = Bytes.toBytes("test_family");

    // create table without setting THEMIS_ENABLE
    deleteTable(admin, testTable);
    HTableDescriptor tableDesc = new HTableDescriptor(testTable);
    HColumnDescriptor columnDesc = new HColumnDescriptor(testFamily);
    tableDesc.addFamily(columnDesc);
    admin.createTable(tableDesc);
    try {
      Get get = new Get(ROW);
      get.addColumn(testFamily, COLUMN.getQualifier());
      cpClient.themisGet(testTable, get, prewriteTs);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("can not access family") >= 0);
    }
    
    HTableInterface table = null;
    try {
      Scan scan = new Scan();
      scan.addColumn(testFamily, COLUMN.getQualifier());
      scan.setAttribute(ThemisScanObserver.TRANSACTION_START_TS, Bytes.toBytes(prewriteTs));
      table = connection.getTable(testTable);
      table.getScanner(scan);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("can not access family") >= 0);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    
    admin.close();
  }
}