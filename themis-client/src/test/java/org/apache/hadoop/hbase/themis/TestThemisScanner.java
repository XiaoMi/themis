package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.themis.ThemisScan;
import org.apache.hadoop.hbase.themis.ThemisScanner;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.cp.ThemisScanObserver;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestThemisScanner extends ClientTestBase {
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    createTransactionWithMock();
  }
  
  @Test
  public void testSetStartTsToScan() {
    Scan scan = new Scan();
    Assert.assertNull(ThemisScanObserver.getStartTsFromAttribute(scan));
    ThemisScanner.setStartTsToScan(scan, PREWRITE_TS);
    Long actual = ThemisScanObserver.getStartTsFromAttribute(scan);
    Assert.assertEquals(PREWRITE_TS, actual.longValue());
  }

  @Test
  public void testCreateGetFromScan() {
    Scan scan = new Scan(ANOTHER_ROW, ROW);
    scan.addColumn(FAMILY, QUALIFIER).addColumn(ANOTHER_FAMILY, ANOTHER_QUALIFIER);
    Get get = ThemisScanner.createGetFromScan(scan, ROW);
    Assert.assertArrayEquals(ROW, get.getRow());
    Assert.assertEquals(2, get.getFamilyMap().size());
    Assert.assertEquals(1, get.getFamilyMap().get(FAMILY).size());
    Assert.assertArrayEquals(QUALIFIER, get.getFamilyMap().get(FAMILY).iterator().next());
    Assert.assertEquals(1, get.getFamilyMap().get(ANOTHER_FAMILY).size());
    Assert.assertArrayEquals(ANOTHER_QUALIFIER,
      get.getFamilyMap().get(ANOTHER_FAMILY).iterator().next());
  }
  
  protected void prepareScanData(ColumnCoordinate[] columns) throws IOException {
    deleteOldDataAndUpdateTs();
    for (ColumnCoordinate columnCoordinate : columns) {
      writePutAndData(columnCoordinate, prewriteTs, commitTs);
    }
    nextTransactionTs();
    createTransactionWithMock();
  }
  
  protected ThemisScan prepareScan(ColumnCoordinate[] columns) throws IOException {
    return prepareScan(columns, null);
  }
  
  protected ThemisScan prepareScan(ColumnCoordinate[] columns, Filter filter) throws IOException {
    ThemisScan pScan = new ThemisScan();
    for (ColumnCoordinate columnCoordinate : columns) {
      pScan.addColumn(columnCoordinate.getFamily(), columnCoordinate.getQualifier());
    }
    if (filter != null) {
      pScan.setFilter(filter);
    }
    return pScan;
  }
  
  protected ThemisScanner prepareScanner(ColumnCoordinate[] columns) throws IOException {
    return prepareScanner(columns, null);
  }
  
  protected ThemisScanner prepareScanner(ColumnCoordinate[] columns, Filter filter) throws IOException {
    return transaction.getScanner(TABLENAME, prepareScan(columns, filter));
  }

  protected void checkScanRow(ColumnCoordinate[] columns, Result result) throws IOException {
    Assert.assertEquals(columns.length, result.size());
    for (ColumnCoordinate columnCoordinate : columns) {
      Assert.assertArrayEquals(VALUE, result.getValue(columnCoordinate.getFamily(), columnCoordinate.getQualifier()));
    }
    for (int i = 0; i < result.list().size(); ++i) {
      Assert.assertEquals(lastTs(prewriteTs), result.list().get(i).getTimestamp());
    }
    Assert.assertArrayEquals(columns[0].getRow(), result.getRow());
  }
  
  protected void checkAndCloseScanner(ThemisScanner scanner) throws IOException {
    Assert.assertNull(scanner.next());
    scanner.close();
  }
  
  @Test
  public void testScanOneRow() throws Exception {
    // null result
    ColumnCoordinate[] columns = new ColumnCoordinate[]{COLUMN};
    ThemisScanner scanner = prepareScanner(columns);
    checkAndCloseScanner(scanner);
    // only one column written
    columns = new ColumnCoordinate[]{COLUMN};
    prepareScanData(columns);
    scanner = prepareScanner(columns);
    Result result = scanner.next();
    checkScanRow(columns, result);
    checkAndCloseScanner(scanner);
    // one row with multi-columns
    columns = new ColumnCoordinate[]{COLUMN, COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER};
    prepareScanData(columns);
    scanner = prepareScanner(columns);
    result = scanner.next();
    checkScanRow(columns, result);
    checkAndCloseScanner(scanner);
  }
  
  protected void checkResultForROW(Result result) throws IOException {
    ColumnCoordinate[] checkColumnsForRow = new ColumnCoordinate[] { COLUMN, COLUMN_WITH_ANOTHER_FAMILY,
        COLUMN_WITH_ANOTHER_QUALIFIER };
    checkScanRow(checkColumnsForRow, result);
  }
  
  @Test
  public void testScanMultiRows() throws Exception {
    // scan multi rows
    prepareScanData(TRANSACTION_COLUMNS);
    ThemisScanner scanner = prepareScanner(TRANSACTION_COLUMNS);
    Result result = scanner.next();
    // should fetch 'AnotherRow' firstly
    checkScanRow(new ColumnCoordinate[]{COLUMN_WITH_ANOTHER_ROW}, result);
    checkResultForROW(scanner.next());
    checkAndCloseScanner(scanner);
    // scan with start-stop row
    ThemisScan pScan = prepareScan(TRANSACTION_COLUMNS);
    pScan.setStartRow(ROW).setStopRow(ROW);
    scanner = transaction.getScanner(TABLENAME, pScan);
    checkResultForROW(scanner.next());
    checkAndCloseScanner(scanner);
  }
  
  @Test
  public void testScanWithDeletedRow() throws Exception {
    nextTransactionTs();
    writePutAndData(COLUMN, prewriteTs - 6, commitTs - 6);
    createTransactionWithMock();
    ThemisScanner scanner = prepareScanner(new ColumnCoordinate[]{COLUMN});
    checkReadColumnResultWithTs(scanner.next(), COLUMN, prewriteTs - 6);
    checkAndCloseScanner(scanner);
    // deleted row is at tail
    writeDeleteColumn(COLUMN, prewriteTs - 3, commitTs - 3);
    scanner = prepareScanner(new ColumnCoordinate[]{COLUMN});
    checkAndCloseScanner(scanner);
    // deleted row is at head
    writeDeleteColumn(COLUMN_WITH_ANOTHER_ROW, prewriteTs - 2, commitTs - 2);
    writePutAndData(COLUMN, prewriteTs - 2, commitTs - 2);
    writePutAndData(COLUMN_WITH_ZZ_ROW, prewriteTs - 2, commitTs - 2);
    scanner = prepareScanner(new ColumnCoordinate[]{COLUMN_WITH_ANOTHER_ROW, COLUMN, COLUMN_WITH_ZZ_ROW});
    checkReadColumnResultWithTs(scanner.next(), COLUMN, prewriteTs - 2);
    checkReadColumnResultWithTs(scanner.next(), COLUMN_WITH_ZZ_ROW, prewriteTs - 2);
    checkAndCloseScanner(scanner);
    scanner.close();
  }
  
  @Test
  public void testScanWithDelete() throws Exception {
    ColumnCoordinate[] scanColumns = new ColumnCoordinate[]{COLUMN, COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER};
    // delete and put both before prewriteTs
    nextTransactionTs();
    createTransactionWithMock();
    writeDeleteAfterPut(COLUMN_WITH_ANOTHER_FAMILY, prewriteTs - 5, commitTs - 5);
    writePutAfterDelete(COLUMN_WITH_ANOTHER_QUALIFIER, prewriteTs - 5, commitTs - 5);
    ThemisScanner scanner = prepareScanner(scanColumns);
    Result result = scanner.next();
    checkReadColumnResultWithTs(result, COLUMN_WITH_ANOTHER_QUALIFIER, prewriteTs - 5);
    checkAndCloseScanner(scanner);
    // delete and put both after prewriteTs
    writeDeleteAfterPut(COLUMN_WITH_ANOTHER_FAMILY, prewriteTs + 1, commitTs + 1);
    writePutAfterDelete(COLUMN_WITH_ANOTHER_QUALIFIER, prewriteTs + 1, commitTs + 1);
    scanner = prepareScanner(scanColumns);
    result = scanner.next();
    checkReadColumnResultWithTs(result, COLUMN_WITH_ANOTHER_FAMILY, prewriteTs - 2);
    checkAndCloseScanner(scanner);
  }
  
  @Test
  public void testScanWithLockClean() throws Exception {
    // lock could be cleaned
    Mockito.when(mockRegister.isWorkerAlive(TestBase.CLIENT_TEST_ADDRESS)).thenReturn(false);
    prepareScanData(TRANSACTION_COLUMNS);
    writeLockAndData(COLUMN, prewriteTs - 5);
    ThemisScan pScan = prepareScan(TRANSACTION_COLUMNS);
    pScan.setCaching(10);
    ThemisScanner scanner = transaction.getScanner(TABLENAME, pScan);
    checkScanRow(new ColumnCoordinate[]{COLUMN_WITH_ANOTHER_ROW}, scanner.next());
    checkResultForROW(scanner.next());
    checkAndCloseScanner(scanner);
    
    // lock can not be cleaned
    writeLockAndData(COLUMN, prewriteTs - 4);
    scanner = transaction.getScanner(TABLENAME, pScan);
    checkScanRow(new ColumnCoordinate[]{COLUMN_WITH_ANOTHER_ROW}, scanner.next());
    Mockito.when(mockRegister.isWorkerAlive(TestBase.CLIENT_TEST_ADDRESS)).thenReturn(true);
    try {
      scanner.next();
      Assert.fail();
    } catch (LockConflictException e) {
    } finally {
      scanner.close();
    }
  }
  
  @Test
  public void testScanWithEmptyRowLeftAfterLockClean() throws Exception {
    prepareScanData(new ColumnCoordinate[]{});
    writeLockAndData(COLUMN_WITH_ANOTHER_ROW, lastTs(prewriteTs));
    Mockito.when(mockRegister.isWorkerAlive(TestBase.CLIENT_TEST_ADDRESS)).thenReturn(false);
    ThemisScan pScan = prepareScan(TRANSACTION_COLUMNS);
    pScan.setCaching(10);
    ThemisScanner scanner = transaction.getScanner(TABLENAME, pScan);
    // no rows left after the lock has been cleaned
    checkAndCloseScanner(scanner);
    
    prepareScanData(new ColumnCoordinate[]{COLUMN, COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER});
    writeLockAndData(COLUMN_WITH_ANOTHER_ROW, lastTs(prewriteTs));
    Mockito.when(mockRegister.isWorkerAlive(TestBase.CLIENT_TEST_ADDRESS)).thenReturn(false);
    pScan = prepareScan(TRANSACTION_COLUMNS);
    pScan.setCaching(10);
    scanner = transaction.getScanner(TABLENAME, pScan);
    checkResultForROW(scanner.next());
    checkAndCloseScanner(scanner);
  }
    
  @Test
  public void testScanWithRowkeyFilter() throws Exception {
    prepareScanData(TRANSACTION_COLUMNS);
    
    ThemisScanner scanner = prepareScanner(TRANSACTION_COLUMNS, new PrefixFilter(ROW));
    checkResultForROW(scanner.next());
    checkAndCloseScanner(scanner);
    
    scanner = prepareScanner(TRANSACTION_COLUMNS, new PrefixFilter(ANOTHER_ROW));
    // should fetch 'AnotherRow' firstly
    checkScanRow(new ColumnCoordinate[]{COLUMN_WITH_ANOTHER_ROW}, scanner.next());
    checkAndCloseScanner(scanner);
    
    FilterList filterList = new FilterList();
    filterList.addFilter(new PrefixFilter(ROW));
    filterList.addFilter(new PrefixFilter(ANOTHER_ROW));
    scanner = prepareScanner(TRANSACTION_COLUMNS, filterList);
    checkAndCloseScanner(scanner);
    
    filterList = new FilterList(Operator.MUST_PASS_ONE);
    filterList.addFilter(new PrefixFilter(ROW));
    filterList.addFilter(new PrefixFilter(ANOTHER_ROW));
    scanner = prepareScanner(TRANSACTION_COLUMNS, filterList);
    // should fetch 'AnotherRow' firstly
    checkScanRow(new ColumnCoordinate[]{COLUMN_WITH_ANOTHER_ROW}, scanner.next());
    checkResultForROW(scanner.next());
    checkAndCloseScanner(scanner);
  }
  
  @Test
  public void testScanWithNoRowkeyFilter() throws Exception {
    prepareScanData(TRANSACTION_COLUMNS);
    ValueFilter valueFilter = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(ANOTHER_VALUE));
    ThemisScanner scanner = prepareScanner(TRANSACTION_COLUMNS, valueFilter);
    checkAndCloseScanner(scanner);
    
    SingleColumnValueFilter singleColumnFilter = new SingleColumnValueFilter(ANOTHER_FAMILY, QUALIFIER,
        CompareOp.EQUAL, VALUE);
    singleColumnFilter.setFilterIfMissing(true);
    scanner = prepareScanner(TRANSACTION_COLUMNS, singleColumnFilter);
    checkResultForROW(scanner.next());
    checkAndCloseScanner(scanner);
    
    valueFilter = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(VALUE));
    singleColumnFilter = new SingleColumnValueFilter(QUALIFIER, QUALIFIER, CompareOp.EQUAL,
        ANOTHER_VALUE);
    singleColumnFilter.setFilterIfMissing(true);
    FilterList filterList = new FilterList();
    filterList.addFilter(valueFilter);
    filterList.addFilter(singleColumnFilter);
    scanner = prepareScanner(TRANSACTION_COLUMNS, filterList);
    checkAndCloseScanner(scanner);
    
    filterList = new FilterList(Operator.MUST_PASS_ONE);
    filterList.addFilter(valueFilter);
    filterList.addFilter(singleColumnFilter);
    scanner = prepareScanner(TRANSACTION_COLUMNS, filterList);
    checkScanRow(new ColumnCoordinate[]{COLUMN_WITH_ANOTHER_ROW}, scanner.next());
    checkResultForROW(scanner.next());
    checkAndCloseScanner(scanner);
  }
  
  @Test
  public void testScanWithFilter() throws IOException {
    prepareScanData(TRANSACTION_COLUMNS);
    writeData(COLUMN, lastTs(prewriteTs), ANOTHER_VALUE);
    ValueFilter valueFilter = new ValueFilter(CompareOp.EQUAL, new BinaryComparator(ANOTHER_VALUE));
    PrefixFilter prefixFilter = new PrefixFilter(ANOTHER_ROW);
    FilterList filterList = new FilterList();
    filterList.addFilter(valueFilter);
    filterList.addFilter(prefixFilter);
    ThemisScanner scanner = prepareScanner(TRANSACTION_COLUMNS, filterList);
    checkAndCloseScanner(scanner);
    
    filterList = new FilterList(Operator.MUST_PASS_ONE);
    filterList.addFilter(valueFilter);
    filterList.addFilter(prefixFilter);
    scanner = prepareScanner(TRANSACTION_COLUMNS, filterList);
    checkScanRow(new ColumnCoordinate[]{COLUMN_WITH_ANOTHER_ROW}, scanner.next());
    Assert.assertEquals(1, scanner.next().size());
    checkAndCloseScanner(scanner);
  }  
}