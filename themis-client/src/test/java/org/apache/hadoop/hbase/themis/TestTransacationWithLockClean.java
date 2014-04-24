package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.TestBase;
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
    TestLockCleaner.setConfigForLockCleaner(conf);
    super.initEnv();
    createTransactionWithMock();
  }
    
  @Test
  public void testPrewriteRowWithLockClean() throws IOException {
    // lock will be cleaned and prewrite will success
    nextTransactionTs();
    preparePrewrite();
    writeLockAndData(COLUMN, prewriteTs - 2);
    ThemisLock lock = getLock(COLUMN);
    Mockito.when(mockRegister.isWorkerAlive(lock.getClientAddress())).thenReturn(false);
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
    Mockito.when(mockClock.getWallTime()).thenReturn(wallTime);
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
    Result result = transaction.get(TABLENAME, getThemisGet(COLUMN));
    Assert.assertNull(readLockBytes(COLUMN, prewriteTs - 2));
    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(VALUE, result.list().get(0).getValue());
    // lock can not be cleaned
    deleteOldDataAndUpdateTs();
    nextTransactionTs();
    createTransactionWithMock();
    writeLockAndData(COLUMN, prewriteTs - 2);
    Mockito.when(mockRegister.isWorkerAlive(TestBase.CLIENT_TEST_ADDRESS)).thenReturn(true);
    Mockito.when(mockClock.getWallTime()).thenReturn(wallTime);
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
    Result result = transaction.tryToCleanLockAndGetAgain(TABLENAME,
      getThemisGet(COLUMN).getHBaseGet(), Arrays.asList(lockKv));
    Assert.assertEquals(1, result.size());
    KeyValue dataKv = result.list().get(0);
    Assert.assertEquals(prewriteTs - 4, dataKv.getTimestamp());
    Assert.assertArrayEquals(VALUE, dataKv.getValue());
  }
}