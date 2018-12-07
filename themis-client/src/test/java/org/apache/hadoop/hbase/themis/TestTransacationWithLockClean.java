package org.apache.hadoop.hbase.themis;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.themis.lockcleaner.TestLockCleaner;
import org.junit.Test;
import org.mockito.Mockito;

public class TestTransacationWithLockClean extends ClientTestBase {

  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    TestLockCleaner.setConfigForLockCleaner(conf);
    createTransactionWithMock();
  }

  // these lock-clean test will use ThemisLock constructed in client-side, so that
  // won't judge lock expired in server-side and won't be affected by the TransactionTTL setting
  @Test
  public void testPrewriteRowWithLockClean() throws IOException {
    // lock will be cleaned and prewrite will success
    nextTransactionTs();
    writeLockAndData(COLUMN, prewriteTs - 2);
    ThemisLock lock = getLock(COLUMN);
    Mockito.when(mockRegister.isWorkerAlive(lock.getClientAddress())).thenReturn(false);
    conf.setBoolean(TransactionConstant.DISABLE_LOCK_CLEAN, true);
    preparePrewrite();
    try {
      transaction.prewriteRowWithLockClean(TABLENAME, transaction.primaryRow, true);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().indexOf("lockClean disabled") >= 0);
    }

    conf.setBoolean(TransactionConstant.DISABLE_LOCK_CLEAN, false);
    preparePrewrite();
    transaction.prewriteRowWithLockClean(TABLENAME, transaction.primaryRow, true);
    assertNull(readLockBytes(COLUMN, prewriteTs - 2));
    checkPrewriteRowSuccess(TABLENAME, transaction.primaryRow);
    // lock can not be cleaned
    deleteOldDataAndUpdateTs();
    nextTransactionTs();
    preparePrewrite();
    writeLockAndData(COLUMN, prewriteTs - 2);
    lock = getLock(COLUMN);
    Mockito.when(mockRegister.isWorkerAlive(lock.getClientAddress())).thenReturn(true);
    long startTs = System.currentTimeMillis();
    try {
      transaction.prewriteRowWithLockClean(TABLENAME, transaction.primaryRow, true);
      fail();
    } catch (LockConflictException e) {
      checkPrewriteColumnSuccess(COLUMN, prewriteTs - 2);
      assertTrue((System.currentTimeMillis() - startTs) >= 100);
    }
  }

  @Test
  public void testGetWithLockClean() throws IOException {
    // lock will be cleaned and get will success
    nextTransactionTs();
    createTransactionWithMock();
    writePutAndData(COLUMN, prewriteTs - 4, commitTs - 4);
    writeLockAndData(COLUMN, prewriteTs - 2);
    Mockito.when(mockRegister.isWorkerAlive(TestBase.CLIENT_TEST_ADDRESS)).thenReturn(false);
    conf.setBoolean(TransactionConstant.DISABLE_LOCK_CLEAN, true);
    createTransactionWithMock();
    try {
      transaction.get(TABLENAME, getThemisGet(COLUMN));
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().indexOf("lockClean disabled") >= 0);
    }
    conf.setBoolean(TransactionConstant.DISABLE_LOCK_CLEAN, false);
    createTransactionWithMock();
    Result result = transaction.get(TABLENAME, getThemisGet(COLUMN));
    assertNull(readLockBytes(COLUMN, prewriteTs - 2));
    assertEquals(1, result.size());
    assertArrayEquals(VALUE, CellUtil.cloneValue(result.rawCells()[0]));
    // lock can not be cleaned
    deleteOldDataAndUpdateTs();
    nextTransactionTs();
    createTransactionWithMock();
    writeLockAndData(COLUMN, prewriteTs - 2);
    Mockito.when(mockRegister.isWorkerAlive(TestBase.CLIENT_TEST_ADDRESS)).thenReturn(true);
    long startTs = System.currentTimeMillis();
    try {
      transaction.get(TABLENAME, getThemisGet(COLUMN));
      fail();
    } catch (LockConflictException e) {
      checkPrewriteColumnSuccess(COLUMN, prewriteTs - 2);
      assertTrue((System.currentTimeMillis() - startTs) >= 100);
    }
  }

  @Test
  public void testTryToCleanLockAndGetAgainWithNewLock() throws IOException {
    // lock will be cleaned, a new lock will be written and get will success
    nextTransactionTs();
    createTransactionWithMock();
    writePutAndData(COLUMN, prewriteTs - 4, commitTs - 4);
    writeLockAndData(COLUMN, prewriteTs - 2);
    writeLockAndData(COLUMN, prewriteTs - 1);
    Mockito.when(mockRegister.isWorkerAlive(TestBase.CLIENT_TEST_ADDRESS)).thenReturn(false);
    Column lc = ColumnUtil.getLockColumn(COLUMN);
    byte[] lockBytes = ThemisLock.toByte(getLock(COLUMN, prewriteTs - 2));
    KeyValue lockKv = new KeyValue(COLUMN.getRow(), lc.getFamily(), lc.getQualifier(),
      prewriteTs - 2, Type.Put, lockBytes);
    conf.setBoolean(TransactionConstant.DISABLE_LOCK_CLEAN, true);
    createTransactionWithMock();
    try {
      transaction.tryToCleanLockAndGetAgain(TABLENAME, getThemisGet(COLUMN).getHBaseGet(),
        Arrays.asList(lockKv));
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().indexOf("lockClean disabled") >= 0);
    }
    conf.setBoolean(TransactionConstant.DISABLE_LOCK_CLEAN, false);
    createTransactionWithMock();
    Result result = transaction.tryToCleanLockAndGetAgain(TABLENAME,
      getThemisGet(COLUMN).getHBaseGet(), Arrays.asList(lockKv));
    assertEquals(1, result.size());
    Cell dataKv = result.rawCells()[0];
    assertEquals(prewriteTs - 4, dataKv.getTimestamp());
    assertArrayEquals(VALUE, CellUtil.cloneValue(dataKv));
  }
}