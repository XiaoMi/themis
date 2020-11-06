package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.themis.lockcleaner.TestLockCleaner;
import org.junit.Assert;
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
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("lockClean disabled") >= 0);
    }
    
    conf.setBoolean(TransactionConstant.DISABLE_LOCK_CLEAN, false);
    preparePrewrite();
    transaction.prewriteRowWithLockClean(TABLENAME, transaction.primaryRow, true);
    Assert.assertNull(readLockBytes(COLUMN, prewriteTs - 2));
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
      Assert.fail();
    } catch (LockConflictException e) {
      checkPrewriteColumnSuccess(COLUMN, prewriteTs - 2);
      Assert.assertTrue((System.currentTimeMillis() - startTs) >= 100);
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
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("lockClean disabled") >= 0);
    }
    conf.setBoolean(TransactionConstant.DISABLE_LOCK_CLEAN, false);
    createTransactionWithMock();
    Result result = transaction.get(TABLENAME, getThemisGet(COLUMN));
    Assert.assertNull(readLockBytes(COLUMN, prewriteTs - 2));
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(VALUE, result.listCells().get(0).getValueArray());
    // lock can not be cleaned
    deleteOldDataAndUpdateTs();
    nextTransactionTs();
    createTransactionWithMock();
    writeLockAndData(COLUMN, prewriteTs - 2);
    Mockito.when(mockRegister.isWorkerAlive(TestBase.CLIENT_TEST_ADDRESS)).thenReturn(true);
    long startTs = System.currentTimeMillis();
    try {
      transaction.get(TABLENAME, getThemisGet(COLUMN));
      Assert.fail();
    } catch (LockConflictException e) {
      checkPrewriteColumnSuccess(COLUMN, prewriteTs - 2);
      Assert.assertTrue((System.currentTimeMillis() - startTs) >= 100);
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
    KeyValue lockKv = new KeyValue(COLUMN.getRow(), lc.getFamily(), lc.getQualifier(), prewriteTs - 2,
        Type.Put, lockBytes);
    conf.setBoolean(TransactionConstant.DISABLE_LOCK_CLEAN, true);
    createTransactionWithMock();
    try {
      transaction.tryToCleanLockAndGetAgain(TABLENAME,
        getThemisGet(COLUMN).getHBaseGet(), Arrays.asList(lockKv));
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("lockClean disabled") >= 0);
    }
    conf.setBoolean(TransactionConstant.DISABLE_LOCK_CLEAN, false);
    createTransactionWithMock();
    Result result = transaction.tryToCleanLockAndGetAgain(TABLENAME,
      getThemisGet(COLUMN).getHBaseGet(), Arrays.asList(lockKv));
    Assert.assertEquals(1, result.size());
    Cell dataKv = result.listCells().get(0);
    Assert.assertEquals(prewriteTs - 4, dataKv.getTimestamp());
    Assert.assertArrayEquals(VALUE, dataKv.getValueArray());
  }
}