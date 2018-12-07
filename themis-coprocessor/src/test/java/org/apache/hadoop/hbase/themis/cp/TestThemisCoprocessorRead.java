package org.apache.hadoop.hbase.themis.cp;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.BinaryComparator;
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
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;

public class TestThemisCoprocessorRead extends TransactionTestBase {
  protected ColumnCoordinate[] columnsWithPutAndDeletePut =
    new ColumnCoordinate[] { COLUMN, COLUMN_WITH_ANOTHER_ROW };

  protected void checkGetsByWriteColumnsKvs(Get get, byte[] row, byte[] family, byte[] qualifier,
      long timestamp) throws IOException {
    assertTrue(Bytes.equals(row, get.getRow()));
    assertEquals(1, get.getFamilyMap().size());
    assertEquals(1, get.getFamilyMap().get(family).size());
    assertTrue(get.getFamilyMap().get(family).contains(qualifier));
    assertEquals(timestamp, get.getTimeRange().getMin());
    assertEquals(timestamp + 1, get.getTimeRange().getMax());
  }

  protected void checkGetOneColumnResult(ColumnCoordinate columnCoordinate, Result internalResult)
      throws IOException {
    assertFalse(ThemisCpUtil.isLockResult(internalResult));
    if (getColumnType(columnCoordinate).equals(Type.Put)) {
      assertEquals(1, internalResult.size());
      Cell kv = internalResult.rawCells()[0];
      assertEquals(columnCoordinate, new ColumnCoordinate(columnCoordinate.getTableName(),
        CellUtil.cloneRow(kv), CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv)));
      assertEquals(prewriteTs, kv.getTimestamp());
      assertArrayEquals(VALUE, CellUtil.cloneValue(kv));
    } else {
      assertEquals(0, internalResult.size());
    }
  }

  @Test
  public void testThemisGetSuccess() throws IOException {
    commitTestTransaction();
    // test get one column
    for (ColumnCoordinate columnCoordinate : TRANSACTION_COLUMNS) {
      Get get = new Get(columnCoordinate.getRow()).addColumn(columnCoordinate.getFamily(),
        columnCoordinate.getQualifier());
      Result iResult = cpClient.themisGet(columnCoordinate.getTableName(), get, commitTs + 1);
      checkGetOneColumnResult(columnCoordinate, iResult);
    }

    // test get one row
    Get get = new Get(COLUMN.getRow());
    get.addColumn(FAMILY, QUALIFIER);
    get.addColumn(FAMILY, ANOTHER_QUALIFIER); // the type of the column is 'DeleteColumn'
    get.addColumn(ANOTHER_FAMILY, QUALIFIER);
    Result iResult = cpClient.themisGet(TABLENAME, get, commitTs + 1);
    assertFalse(ThemisCpUtil.isLockResult(iResult));
    assertEquals(2, iResult.size());
    for (Cell kv : iResult.rawCells()) {
      assertEquals(prewriteTs, kv.getTimestamp());
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
      fail();
    } catch (Exception e) {
    }
    // unknown qualifier
    get = new Get(COLUMN.getRow());
    get.addColumn(FAMILY, QUALIFIER);
    byte[] NULL_QUALIFIER = Bytes.toBytes("Null" + Bytes.toString(QUALIFIER));
    get.addColumn(FAMILY, NULL_QUALIFIER);
    Result iResult = cpClient.themisGet(TABLENAME, get, commitTs + 1);
    assertFalse(ThemisCpUtil.isLockResult(iResult));
    assertEquals(1, iResult.size());
    assertEquals(prewriteTs, iResult.rawCells()[0].getTimestamp());
  }

  protected void commitColumnsWithDifferentTs() throws IOException {
    commitOneColumn(COLUMN, Type.Put);
    commitOneColumn(COLUMN_WITH_ANOTHER_FAMILY, Type.Put);
    nextTransactionTs();
    commitOneColumn(COLUMN_WITH_ANOTHER_FAMILY, Type.Put);
    commitOneColumn(COLUMN_WITH_ANOTHER_QUALIFIER, Type.Put);
    nextTransactionTs();
  }

  protected void checkGetResultForDifferentTs(Result result) throws IOException {
    assertFalse(ThemisCpUtil.isLockResult(result));
    assertEquals(3, result.size());
    assertEquals(1, result.getColumnCells(COLUMN.getFamily(), COLUMN.getQualifier()).size());
    assertEquals(prewriteTs - 200,
      result.getColumnCells(COLUMN.getFamily(), COLUMN.getQualifier()).get(0).getTimestamp());
    for (ColumnCoordinate columnCoordinate : new ColumnCoordinate[] { COLUMN_WITH_ANOTHER_FAMILY,
      COLUMN_WITH_ANOTHER_QUALIFIER }) {
      assertEquals(1, result
        .getColumnCells(columnCoordinate.getFamily(), columnCoordinate.getQualifier()).size());
      assertEquals(prewriteTs - 100,
        result.getColumnCells(columnCoordinate.getFamily(), columnCoordinate.getQualifier()).get(0)
          .getTimestamp());
    }
  }

  protected Get createGetForDifferentTs() {
    Get get = new Get(ROW);
    get.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
    get.addColumn(COLUMN_WITH_ANOTHER_FAMILY.getFamily(),
      COLUMN_WITH_ANOTHER_FAMILY.getQualifier());
    get.addColumn(COLUMN_WITH_ANOTHER_QUALIFIER.getFamily(),
      COLUMN_WITH_ANOTHER_QUALIFIER.getQualifier());
    return get;
  }

  @Test
  public void testGetForColumnsWithDifferentTs() throws IOException {
    commitColumnsWithDifferentTs();
    Result iResult = cpClient.themisGet(TABLENAME, createGetForDifferentTs(), prewriteTs);
    checkGetResultForDifferentTs(iResult);
  }

  protected void checkGetOneColumnConflictResult(ColumnCoordinate columnCoordinate,
      Result internalResult, long prewriteTs) throws IOException {
    assertTrue(ThemisCpUtil.isLockResult(internalResult));
    assertEquals(1, internalResult.size());
    Cell kv = internalResult.rawCells()[0];
    assertTrue(ColumnUtil.getLockColumn(columnCoordinate)
      .equals(new ColumnCoordinate(columnCoordinate.getTableName(), CellUtil.cloneRow(kv),
        CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv))));
    assertEquals(prewriteTs, kv.getTimestamp());
    assertArrayEquals(ThemisLock.toByte(getLock(columnCoordinate, prewriteTs)),
      CellUtil.cloneValue(kv));

  }

  protected void checkGetOneColumnConflictResult(ColumnCoordinate columnCoordinate,
      Result internalResult) throws IOException {
    checkGetOneColumnConflictResult(columnCoordinate, internalResult, prewriteTs);
  }

  @Test
  public void testThemisGetWithOneColumnLockConflict() throws IOException {
    nextTransactionTs();
    // test one column
    for (ColumnCoordinate columnCoordinate : TRANSACTION_COLUMNS) {
      writeLockAndData(columnCoordinate);
      Get get = new Get(columnCoordinate.getRow()).addColumn(columnCoordinate.getFamily(),
        columnCoordinate.getQualifier());
      // will encounter lock conflict
      Result iResult = cpClient.themisGet(columnCoordinate.getTableName(), get, commitTs);
      checkGetOneColumnConflictResult(columnCoordinate, iResult);
      // newer lock, won't encounter lock conflict
      iResult = cpClient.themisGet(columnCoordinate.getTableName(), get, prewriteTs);
      assertFalse(ThemisCpUtil.isLockResult(iResult));
      assertTrue(iResult.isEmpty());
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
    assertFalse(ThemisCpUtil.isLockResult(iResult));
    assertEquals(2, iResult.size());
    // a column with newer lock, won't cause lock conflict
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertFalse(ThemisCpUtil.isLockResult(iResult));
    assertEquals(2, iResult.size());
  }

  @Test
  public void testGetOneCellFromMultiColumnsRow() throws IOException {
    commitOneColumn(new ColumnCoordinate(TABLENAME, ROW, ANOTHER_FAMILY, ANOTHER_QUALIFIER),
      Type.Put);
    commitOneColumn(COLUMN_WITH_ANOTHER_FAMILY, Type.Put);
    commitOneColumn(COLUMN_WITH_ANOTHER_QUALIFIER, Type.Put);
    commitOneColumn(COLUMN, Type.Put);
    nextTransactionTs();
    Get get = new Get(ROW);
    get.addColumn(COLUMN_WITH_ANOTHER_QUALIFIER.getFamily(),
      COLUMN_WITH_ANOTHER_QUALIFIER.getQualifier());
    Result result = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertEquals(1, result.size());
    get = new Get(ROW);
    get.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
    result = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertEquals(1, result.size());
    get = new Get(ROW);
    get.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
    get.addColumn(COLUMN_WITH_ANOTHER_QUALIFIER.getFamily(),
      COLUMN_WITH_ANOTHER_QUALIFIER.getQualifier());
    result = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertEquals(2, result.size());
  }

  @Test
  public void testColumnTimestampFilterInGet() throws IOException {
    commitOneColumn(COLUMN, Type.Put, prewriteTs, commitTs);
    // ColumnTimestampFilter should filter this cell
    commitOneColumn(COLUMN, Type.Put, prewriteTs + 20, prewriteTs + 50);
    commitOneColumn(COLUMN_WITH_ANOTHER_FAMILY, Type.Put, prewriteTs + 30, prewriteTs + 40);
    Get get = new Get(ROW);
    get.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
    get.addColumn(COLUMN_WITH_ANOTHER_FAMILY.getFamily(),
      COLUMN_WITH_ANOTHER_FAMILY.getQualifier());
    Result result = cpClient.themisGet(TABLENAME, get, prewriteTs + 50);
    assertFalse(ThemisCpUtil.isLockResult(result));
    assertEquals(2, result.size());
    assertEquals(prewriteTs,
      result.getColumnLatestCell(COLUMN.getFamily(), COLUMN.getQualifier()).getTimestamp());
    assertEquals(prewriteTs + 30, result.getColumnLatestCell(COLUMN_WITH_ANOTHER_FAMILY.getFamily(),
      COLUMN_WITH_ANOTHER_FAMILY.getQualifier()).getTimestamp());
  }

  @Test
  public void testGetFamily() throws IOException {
    // get entire row
    commitTestTransaction();
    Get get = new Get(ROW);
    Result iResult = cpClient.themisGet(TABLENAME, get, prewriteTs + 50);
    assertFalse(ThemisCpUtil.isLockResult(iResult));
    assertEquals(2, iResult.size());
    checkResultKvColumn(COLUMN_WITH_ANOTHER_FAMILY, iResult.rawCells()[0]);
    checkResultKvColumn(COLUMN, iResult.rawCells()[1]);

    // get entire family
    commitOneColumn(COLUMN_WITH_ANOTHER_QUALIFIER, Type.Put, prewriteTs + 10, commitTs + 10);
    get = new Get(ROW);
    get.addFamily(FAMILY);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs + 50);
    assertEquals(2, iResult.size());
    checkResultKvColumn(COLUMN_WITH_ANOTHER_QUALIFIER, iResult.rawCells()[0]);
    checkResultKvColumn(COLUMN, iResult.rawCells()[1]);

    // get entire family together with another family's column
    get = new Get(ROW);
    get.addFamily(FAMILY);
    get.addColumn(ANOTHER_FAMILY, QUALIFIER);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs + 50);
    assertEquals(3, iResult.size());
    checkResultKvColumn(COLUMN_WITH_ANOTHER_FAMILY, iResult.rawCells()[0]);
    checkResultKvColumn(COLUMN_WITH_ANOTHER_QUALIFIER, iResult.rawCells()[1]);
    checkResultKvColumn(COLUMN, iResult.rawCells()[2]);

    // get entire family with lock in another family
    writeLockAndData(COLUMN_WITH_ANOTHER_FAMILY, prewriteTs + 20);
    get = new Get(ROW);
    get.addFamily(FAMILY);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs + 50);
    assertEquals(2, iResult.size());
    checkResultKvColumn(COLUMN_WITH_ANOTHER_QUALIFIER, iResult.rawCells()[0]);
    checkResultKvColumn(COLUMN, iResult.rawCells()[1]);

    // get entire family with lock conflict
    get = new Get(ROW);
    get.addFamily(FAMILY);
    get.addColumn(ANOTHER_FAMILY, QUALIFIER);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs + 50);
    checkGetOneColumnConflictResult(COLUMN_WITH_ANOTHER_FAMILY, iResult, prewriteTs + 20);

    // get entire family with lock in another column which is not requested
    get = new Get(ROW);
    get.addFamily(FAMILY);
    get.addColumn(ANOTHER_FAMILY, ANOTHER_QUALIFIER);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs + 50);
    assertEquals(2, iResult.size());
    checkResultKvColumn(COLUMN_WITH_ANOTHER_QUALIFIER, iResult.rawCells()[0]);
    checkResultKvColumn(COLUMN, iResult.rawCells()[1]);
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
    assertTrue(iResult.isEmpty());
    FilterList filterList = new FilterList();
    filterList.addFilter(new PrefixFilter(ROW));
    filterList.addFilter(new PrefixFilter(ANOTHER_ROW));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertTrue(iResult.isEmpty());
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
    get.setFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    Result iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertTrue(iResult.isEmpty());
    // test SingleColumnValueFilter
    get.setFilter(
      new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, ANOTHER_VALUE));
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertTrue(iResult.isEmpty());

    writeData(COLUMN, prewriteTs - 200, ANOTHER_VALUE);
    writePutColumn(COLUMN, prewriteTs - 200, commitTs - 200);
    get.setFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertEquals(1, iResult.size());
    assertFalse(ThemisCpUtil.isLockResult(iResult));
    assertEquals(prewriteTs - 200, iResult.rawCells()[0].getTimestamp());

    get.setFilter(
      new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, ANOTHER_VALUE));
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);

    // test FilterList with MUST_PASS_ALL
    FilterList filterList = new FilterList();
    filterList
      .addFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    filterList.addFilter(
      new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, ANOTHER_VALUE));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertEquals(1, iResult.size());
    assertFalse(ThemisCpUtil.isLockResult(iResult));
    assertEquals(prewriteTs - 200, iResult.rawCells()[0].getTimestamp());

    // test FilterList with MUST_PASS_ONE
    filterList = new FilterList(Operator.MUST_PASS_ONE);
    filterList
      .addFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    filterList.addFilter(
      new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, ANOTHER_VALUE));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);

    // test transfer filter
    get.setFilter(new KeyOnlyFilter());
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);
    for (Cell kv : iResult.rawCells()) {
      assertEquals(0, kv.getValueLength());
    }
  }

  @Test
  public void testGetWithFilter() throws IOException {
    // rowkey filter pass while column filter not pass
    commitColumnsWithDifferentTs();
    Get get = createGetForDifferentTs();
    FilterList filterList = new FilterList();
    filterList.addFilter(new PrefixFilter(ROW));
    filterList
      .addFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    get.setFilter(filterList);
    Result iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertTrue(iResult.isEmpty());

    filterList = new FilterList(Operator.MUST_PASS_ONE);
    filterList.addFilter(new PrefixFilter(ROW));
    filterList
      .addFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(ANOTHER_VALUE)));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);

    // rowkey filter not pass while column filter pass
    filterList = new FilterList();
    filterList.addFilter(new PrefixFilter(ANOTHER_ROW));
    filterList.addFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(VALUE)));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    assertTrue(iResult.isEmpty());

    filterList = new FilterList(Operator.MUST_PASS_ONE);
    filterList.addFilter(new PrefixFilter(ANOTHER_ROW));
    filterList.addFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(VALUE)));
    get.setFilter(filterList);
    iResult = cpClient.themisGet(TABLENAME, get, prewriteTs);
    checkGetResultForDifferentTs(iResult);
  }

  @Test
  public void testGetLockAndErase() throws IOException {
    // test without lock
    assertNull(cpClient.getLockAndErase(COLUMN, prewriteTs));
    // test with lock
    nextTransactionTs();
    writeLockAndData(COLUMN);
    ThemisLock lock = cpClient.getLockAndErase(COLUMN, prewriteTs);
    assertTrue(lock.equals(getLock(COLUMN)));
    assertNull(readLockBytes(COLUMN));
  }

  @Test
  public void testWriteNonThemisFamily() throws IOException {
    TableName testTable = TableName.valueOf("test_table");
    byte[] testFamily = Bytes.toBytes("test_family");
    // create table without setting THEMIS_ENABLE
    deleteTable(testTable);

    try (Admin admin = connection.getAdmin()) {
      admin.createTable(TableDescriptorBuilder.newBuilder(testTable)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(testFamily)).build());
    }
    try {
      Get get = new Get(ROW);
      get.addColumn(testFamily, COLUMN.getQualifier());
      cpClient.themisGet(testTable, get, prewriteTs);
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("can not access family"));
    }
    Scan scan = new Scan();
    scan.addColumn(testFamily, COLUMN.getQualifier());
    scan.setAttribute(ThemisScanObserver.TRANSACTION_START_TS, Bytes.toBytes(prewriteTs));
    try (Table table = connection.getTable(testTable)) {
      table.getScanner(scan);
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("can not access family"));
    }
  }

  @Test
  public void testExpiredGet() throws IOException {
    // only test in MiniCluster
    if (TEST_UTIL != null) {
      // won't expired
      long currentMs = System.currentTimeMillis() + TransactionTTL.writeTransactionTTL;
      prewriteTs = TransactionTTL.getExpiredTimestampForReadByCommitColumn(currentMs);
      Get get = new Get(ROW);
      get.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
      cpClient.themisGet(TABLENAME, get, prewriteTs);

      // make sure this transaction will be expired
      currentMs = System.currentTimeMillis() - TransactionTTL.transactionTTLTimeError;
      prewriteTs = TransactionTTL.getExpiredTimestampForReadByCommitColumn(currentMs);
      try {
        cpClient.themisGet(TABLENAME, get, prewriteTs);
        fail();
      } catch (IOException e) {
        assertTrue(e.getMessage().indexOf("Expired Read Transaction") >= 0);
      }
    }
  }

  @Test
  public void testExpiredScan() throws IOException {
    // only test in MiniCluster
    if (TEST_UTIL != null) {
      prewritePrimaryRow();
      commitPrimaryRow();
      // won't expired
      long currentMs = System.currentTimeMillis() + TransactionTTL.writeTransactionTTL;
      prewriteTs = TransactionTTL.getExpiredTimestampForReadByCommitColumn(currentMs);
      Scan scan = new Scan();
      scan.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
      scan.setAttribute(ThemisScanObserver.TRANSACTION_START_TS, Bytes.toBytes(prewriteTs));
      ResultScanner scanner = getTable(TABLENAME).getScanner(scan);
      scanner.next();
      scanner.close();
      scanner = null;
      // make sure this transaction will be expired
      currentMs = System.currentTimeMillis() - TransactionTTL.transactionTTLTimeError;
      prewriteTs = TransactionTTL.getExpiredTimestampForReadByCommitColumn(currentMs);
      scan = new Scan();
      scan.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
      scan.setAttribute(ThemisScanObserver.TRANSACTION_START_TS, Bytes.toBytes(prewriteTs));
      try {
        scanner = getTable(TABLENAME).getScanner(scan);
        scanner.next();
        fail();
      } catch (IOException e) {
        assertTrue(e.getMessage().indexOf("Expired Read Transaction") >= 0);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }

      // getScanner won't expired, next will
      currentMs = System.currentTimeMillis() + 3 * 1000; // assume the rpc will be completed in 3
                                                         // seconds
      prewriteTs = TransactionTTL.getExpiredTimestampForReadByCommitColumn(currentMs);
      scan = new Scan();
      scan.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
      scan.setAttribute(ThemisScanObserver.TRANSACTION_START_TS, Bytes.toBytes(prewriteTs));
      scanner = getTable(TABLENAME).getScanner(scan);
      Threads.sleep(5000); // to cause the read expired
      try {
        scanner.next();
        fail();
      } catch (IOException e) {
        assertTrue(e.getMessage().indexOf("Expired Read Transaction") >= 0);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    }
  }

  /*
   * @Test public void testIsLockExpired() throws IOException { if (TEST_UTIL != null) {
   * TransactionTTL.init(conf); long ts =
   * TransactionTTL.getExpiredTimestampForWrite(System.currentTimeMillis() +
   * TransactionTTL.transactionTTLTimeError); assertFalse(cpClient.isLockExpired(TABLENAME, ROW,
   * ts)); ts = TransactionTTL.getExpiredTimestampForWrite(System.currentTimeMillis() -
   * TransactionTTL.transactionTTLTimeError); assertTrue(cpClient.isLockExpired(TABLENAME, ROW,
   * ts)); } }
   */
}
