package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.TestThemisCpUtil;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpointClient;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.themis.lock.PrimaryLock;
import org.apache.hadoop.hbase.themis.lock.SecondaryLock;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

public class TestLockCleaner extends ClientTestBase {
  protected ThemisEndpointClient cpClient;
  protected LockCleaner lockCleaner;
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    setConfigForLockCleaner(conf);
    cpClient = new ThemisEndpointClient(connection);
    lockCleaner = new LockCleaner(conf, connection, mockClock, mockRegister, cpClient);
  }
  
  public static void setConfigForLockCleaner(Configuration conf) {
    conf.setInt(TransactionConstant.THEMIS_LOCK_TTL_KEY, 100);
    conf.setInt(TransactionConstant.THEMIS_RETRY_COUNT, 2);    
  }

  @Test
  public void testIsLockExpired() throws IOException {
    long wallTime = System.currentTimeMillis();
    ThemisLock lock = getLock(COLUMN);
    lock.setWallTime(wallTime);
    Mockito.when(mockClock.getWallTime()).thenReturn(wallTime - 1);
    Assert.assertFalse(lockCleaner.isLockExpired(lock));
    Mockito.when(mockClock.getWallTime()).thenReturn(wallTime + 99);
    Assert.assertFalse(lockCleaner.isLockExpired(lock));
    Mockito.when(mockClock.getWallTime()).thenReturn(wallTime + 100);
    Assert.assertTrue(lockCleaner.isLockExpired(lock));
    Mockito.when(mockClock.getWallTime()).thenReturn(wallTime + 101);
    Assert.assertTrue(lockCleaner.isLockExpired(lock));
  }
  

  @Test
  public void testShouldCleanLock() throws IOException {
    long wallTime = System.currentTimeMillis();
    ThemisLock lock = getLock(COLUMN);
    lock.setWallTime(wallTime);
    Mockito.when(mockRegister.isWorkerAlive(lock.getClientAddress())).thenReturn(true);
    Mockito.when(mockClock.getWallTime()).thenReturn(wallTime + 99);
    Assert.assertFalse(lockCleaner.shouldCleanLock(lock));
    Mockito.when(mockClock.getWallTime()).thenReturn(wallTime + 100);
    Assert.assertTrue(lockCleaner.shouldCleanLock(lock));
    Mockito.when(mockRegister.isWorkerAlive(lock.getClientAddress())).thenReturn(false);
    Mockito.when(mockClock.getWallTime()).thenReturn(wallTime + 99);
    Assert.assertTrue(lockCleaner.shouldCleanLock(lock));
    Mockito.when(mockClock.getWallTime()).thenReturn(wallTime + 100);
    Assert.assertTrue(lockCleaner.shouldCleanLock(lock));
  }

  @Test
  public void testGetPrimaryLockWithColumn() throws IOException {
    PrimaryLock primary = (PrimaryLock)getLock(COLUMN);
    PrimaryLock actual = LockCleaner.getPrimaryLockWithColumn(primary);
    Assert.assertTrue(primary.equals(actual));
    Assert.assertTrue(COLUMN.equals(actual.getColumn()));
    SecondaryLock secondary = (SecondaryLock)getLock(COLUMN_WITH_ANOTHER_ROW);
    actual = LockCleaner.getPrimaryLockWithColumn(secondary);
    Assert.assertTrue(COLUMN.equals(actual.getColumn()));
    Assert.assertTrue(actual.isPrimary());
    Assert.assertEquals(Type.Minimum, actual.getType());
    Assert.assertEquals(1, actual.getSecondaryColumns().size());
    Assert.assertNotNull(actual.getSecondaryColumns().get(COLUMN_WITH_ANOTHER_ROW));
  }

  @Test
  public void testCreateGetOfWriteColumnsIndexingPrewriteTs() throws IOException {
    Get get = lockCleaner.createGetOfWriteColumnsIndexingPrewriteTs(COLUMN, prewriteTs);
    TestThemisCpUtil.checkReadWithWriteColumns(get.getFamilyMap(), COLUMN);
    Assert.assertEquals(prewriteTs, get.getTimeRange().getMin());
    Assert.assertEquals(Long.MAX_VALUE, get.getTimeRange().getMax());
    Assert.assertEquals(Integer.MAX_VALUE, get.getMaxVersions());
  }
  
  @Test
  public void testGetTimestampOfWriteIndexingPrewriteTs() throws IOException {
    // write column is null
    Assert.assertNull(lockCleaner.getTimestampOfWriteIndexingPrewriteTs(COLUMN, prewriteTs));
    // value of write column not equal to prewriteTs
    writePutColumn(COLUMN, prewriteTs - 1, commitTs - 1);
    writePutColumn(COLUMN, prewriteTs + 1, commitTs + 1);
    writeDeleteColumn(COLUMN, prewriteTs + 2, commitTs + 2);
    Assert.assertNull(lockCleaner.getTimestampOfWriteIndexingPrewriteTs(COLUMN, prewriteTs));
    // value of write column equals to prewriteTs
    writePutColumn(COLUMN, prewriteTs, commitTs);
    Long ts = lockCleaner.getTimestampOfWriteIndexingPrewriteTs(COLUMN, prewriteTs);
    Assert.assertEquals(commitTs, ts.longValue());
    ts = lockCleaner.getTimestampOfWriteIndexingPrewriteTs(COLUMN, prewriteTs + 2);
    Assert.assertEquals(commitTs + 2, ts.longValue());
  }

  @Test
  public void testCleanPrimaryLock() throws IOException {
    // lock exist
    writeLockAndData(COLUMN);
    Pair<Long, PrimaryLock> result = lockCleaner.cleanPrimaryLock(COLUMN, prewriteTs);
    Assert.assertNull(result.getFirst());
    Assert.assertEquals(getLock(COLUMN), result.getSecond());
    Assert.assertNull(readLockBytes(COLUMN));
    // transaction committed with lock cleaned
    boolean[] isPuts = new boolean[]{true, false};
    for (boolean isPut : isPuts) {
      deleteOldDataAndUpdateTs();
      writeWriteColumn(COLUMN, prewriteTs, commitTs, isPut);
      result = lockCleaner.cleanPrimaryLock(COLUMN, prewriteTs);
      Assert.assertEquals(commitTs, result.getFirst().longValue());
      Assert.assertNull(result.getSecond());
    }
    // transaction uncommitted with lock erased
    deleteOldDataAndUpdateTs();
    result = lockCleaner.cleanPrimaryLock(COLUMN, prewriteTs);
    Assert.assertNull(result.getFirst());
    Assert.assertNull(result.getSecond());
  }
  
  protected void prepareCleanSecondaryLocks(boolean committed) throws IOException {
    if (committed) {
      writePutAndData(SECONDARY_COLUMNS[0], prewriteTs, commitTs);
    } else {
      writeData(SECONDARY_COLUMNS[0], prewriteTs);
    }
    for (int i = 1; i < SECONDARY_COLUMNS.length; ++i) {
      writeLockAndData(SECONDARY_COLUMNS[i]);
    }
  }
  
  @Test
  public void testEraseAndDataLock() throws IOException {
    writeLockAndData(COLUMN);
    lockCleaner.eraseLockAndData(COLUMN, prewriteTs);
    Assert.assertNull(readDataValue(COLUMN, prewriteTs));
    Assert.assertNull(readLockBytes(COLUMN));
    
    // erase lock by row
    for (ColumnCoordinate columnCoordinate : new ColumnCoordinate[]{COLUMN, COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER}) {
      writeLockAndData(columnCoordinate);
    }
    lockCleaner.eraseLockAndData(TABLENAME, PRIMARY_ROW.getRow(), PRIMARY_ROW.getColumns(), prewriteTs);
    for (ColumnCoordinate columnCoordinate : new ColumnCoordinate[]{COLUMN, COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER}) {
      Assert.assertNull(readLockBytes(columnCoordinate));
    }
  }
  @Test
  public void testCleanSecondaryLocks() throws IOException {
    // check commit secondary
    prepareCleanSecondaryLocks(true);
    lockCleaner.cleanSecondaryLocks((PrimaryLock)getLock(COLUMN), commitTs);
    checkCommitSecondariesSuccess();
    // check erase secondary lock
    deleteOldDataAndUpdateTs();
    prepareCleanSecondaryLocks(false);
    lockCleaner.cleanSecondaryLocks((PrimaryLock)getLock(COLUMN), null);
    checkSecondariesRollback();
  }

  @Test
  public void testCleanLockFromPrimary() throws IOException {
    ThemisLock from = getLock(COLUMN);
    // primary lock exist
    writeLockAndData(COLUMN);
    prepareCleanSecondaryLocks(false);
    lockCleaner.cleanLock(from);
    checkTransactionRollback();
    // transaction committed with primary lock cleaned
    deleteOldDataAndUpdateTs();
    writePutAndData(COLUMN, prewriteTs, commitTs);
    prepareCleanSecondaryLocks(true);
    from = getLock(COLUMN);
    lockCleaner.cleanLock(from);
    checkTransactionCommitSuccess();
    // transaction uncommitted with primary lock erased
    deleteOldDataAndUpdateTs();
    writeData(COLUMN, prewriteTs);
    prepareCleanSecondaryLocks(false);
    from = getLock(COLUMN);
    lockCleaner.cleanLock(from);
    checkTransactionRollback();
  }
  
  protected void checkColumnsCommitSuccess(ColumnCoordinate[] columns) throws IOException {
    for (ColumnCoordinate columnCoordinate : columns) {
      checkCommitColumnSuccess(columnCoordinate);
    }
  }
  
  protected void checkColumnsPrewriteSuccess(ColumnCoordinate[] columns) throws IOException {
    for (ColumnCoordinate columnCoordinate : columns) {
      checkPrewriteColumnSuccess(columnCoordinate);
    }
  }
  
  protected void checkColumnsRallback(ColumnCoordinate[] columns) throws IOException {
    for (ColumnCoordinate columnCoordinate : columns) {
      checkColumnRollback(columnCoordinate);
    }
  }
  
  @Test
  public void testCleanLockFromSecondary() throws IOException {
    ColumnCoordinate columnCoordinate = SECONDARY_COLUMNS[1];
    ThemisLock from = getLock(columnCoordinate);
    // primary lock exist
    writeLockAndData(COLUMN);
    prepareCleanSecondaryLocks(false);
    lockCleaner.cleanLock(from);
    checkTransactionRollback();
    // transaction committed with primary lock cleaned
    deleteOldDataAndUpdateTs();
    writePutAndData(COLUMN, prewriteTs, commitTs);
    prepareCleanSecondaryLocks(true);
    from = getLock(columnCoordinate);
    lockCleaner.cleanLock(from);
    checkColumnsCommitSuccess(new ColumnCoordinate[]{COLUMN, SECONDARY_COLUMNS[0], SECONDARY_COLUMNS[1]});
    checkColumnsPrewriteSuccess(new ColumnCoordinate[]{SECONDARY_COLUMNS[2], SECONDARY_COLUMNS[3]});
    // transaction uncommitted with primary lock erased
    deleteOldDataAndUpdateTs();
    writeData(COLUMN, prewriteTs);
    prepareCleanSecondaryLocks(false);
    from = getLock(columnCoordinate);
    lockCleaner.cleanLock(from);
    checkColumnsRallback(new ColumnCoordinate[]{COLUMN, SECONDARY_COLUMNS[0], SECONDARY_COLUMNS[1]});
    checkColumnsPrewriteSuccess(new ColumnCoordinate[]{SECONDARY_COLUMNS[2], SECONDARY_COLUMNS[3]});
  }

  class TryToCleanLockThread extends Thread {
    private ThemisLock lock;
    private boolean cleanLocks;
    public TryToCleanLockThread(ThemisLock lock, boolean cleanLocks) {
      this.lock = lock;
      this.cleanLocks = cleanLocks;
    }
    @Override
    public void run() {
      try {
        invokeTryToCleanLock(lock, cleanLocks);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  protected void invokeTryToCleanLock(ThemisLock lock, boolean cleanLocks) throws IOException {
    ColumnCoordinate c = lock.getColumn();
    if (cleanLocks) {
      Column lockColumn = ColumnUtil.getLockColumn(c);
      KeyValue kv = new KeyValue(c.getRow(), lockColumn.getFamily(),
          lockColumn.getQualifier(), lock.getTimestamp(), Type.Put, ThemisLock.toByte(lock));
      lockCleaner.tryToCleanLocks(c.getTableName(), Lists.newArrayList(kv));
    } else {
      lockCleaner.tryToCleanLock(lock);
    }
  }
  
  @Test
  public void testTryToCleanLockSuccess() throws Exception {
    boolean[] cleanLocksOptions = new boolean[]{false, true};
    for (boolean cleanLocks : cleanLocksOptions) {
      // lock should be cleaned and clean successful
      ThemisLock lc = getLock(COLUMN);
      writeLockAndData(COLUMN);
      Mockito.when(mockRegister.isWorkerAlive(lc.getClientAddress())).thenReturn(false);
      invokeTryToCleanLock(lc, cleanLocks);
      checkColumnRollback(COLUMN);
      // lock should be cleaned after retry
      deleteOldDataAndUpdateTs();
      lc = getLock(COLUMN);
      lc.setWallTime(wallTime - 50);
      writeLockAndData(COLUMN);
      Mockito.when(mockRegister.isWorkerAlive(lc.getClientAddress())).thenReturn(true);
      // the lock won't be expire when try to clean lock firstly
      Mockito.when(mockClock.getWallTime()).thenReturn(wallTime);
      TryToCleanLockThread cleanThread = new TryToCleanLockThread(lc, cleanLocks);
      long startTs = System.currentTimeMillis();
      cleanThread.start();
      Threads.sleep(50);
      Mockito.when(mockClock.getWallTime()).thenReturn(wallTime + 51);
      cleanThread.join();
      checkColumnRollback(COLUMN);
      Assert.assertTrue(System.currentTimeMillis() - startTs >= 100);
      // lock won't be cleaned by this transaction, however, cleaned by another transaction
      deleteOldDataAndUpdateTs();
      lc = getLock(COLUMN);
      Mockito.when(mockRegister.isWorkerAlive(lc.getClientAddress())).thenReturn(true);
      Mockito.when(mockClock.getWallTime()).thenReturn(wallTime);
      invokeTryToCleanLock(lc, cleanLocks);
    }
  }
  
  @Test
  public void testTryToCleanLockFail() throws Exception {
    // lock with null column
    ThemisLock nullColumnLock = getLock(COLUMN);
    nullColumnLock.setColumn(null);
    try {
      lockCleaner.tryToCleanLock(nullColumnLock);
      Assert.fail();
    } catch (ThemisFatalException e) {}
    
    boolean[] cleanLocksOptions = new boolean[]{false, true};
    for (boolean cleanLocks : cleanLocksOptions) {
      // lock should be cleaned but clean fail
      ThemisLock lc = getLock(COLUMN);
      writeLockAndData(COLUMN);
      Mockito.when(mockRegister.isWorkerAlive(lc.getClientAddress())).thenReturn(false);
      HBaseAdmin admin = new HBaseAdmin(connection.getConfiguration());
      admin.disableTable(TABLENAME);
      try {
        invokeTryToCleanLock(lc, cleanLocks);
      } catch (IOException e) {
        admin.enableTable(TABLENAME);
        e.printStackTrace();
        Assert.assertTrue(e.getCause() instanceof RetriesExhaustedException);
        checkPrewriteColumnSuccess(COLUMN);
      } finally {
        admin.close();
      }
      // lock should not be cleaned after retry
      deleteOldDataAndUpdateTs();
      writeLockAndData(COLUMN);
      lc = getLock(COLUMN);
      lc.setWallTime(wallTime - 50);
      Mockito.when(mockRegister.isWorkerAlive(lc.getClientAddress())).thenReturn(true);
      Mockito.when(mockClock.getWallTime()).thenReturn(wallTime);
      long startTs = System.currentTimeMillis();
      try {
        invokeTryToCleanLock(lc, cleanLocks);
        Assert.fail();
      } catch (LockConflictException e) {
        checkPrewriteColumnSuccess(COLUMN);
        Assert.assertTrue((System.currentTimeMillis() - startTs) >= 100);
      }
    }
  }
  
  @Test
  public void testHasLock() throws IOException {
    ThemisLock lc = getLock(COLUMN);
    Assert.assertFalse(lockCleaner.hasLock(lc));
    writeLockAndData(COLUMN);
    Assert.assertTrue(lockCleaner.hasLock(lc));
  }
  
  @Test
  public void testConstructLocks() throws IOException {
    ThemisLock expectLock = getPrimaryLock();
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(getLockKv(KEYVALUE, ThemisLock.toByte(expectLock)));
    List<ThemisLock> locks = LockCleaner.constructLocks(TABLENAME, kvs);
    Assert.assertEquals(1, locks.size());
    Assert.assertTrue(expectLock.equals(locks.get(0)));
    Assert.assertTrue(COLUMN.equals(locks.get(0).getColumn()));
    
    // with no-lock column, should throw exception
    kvs.add(KEYVALUE);
    try {
      LockCleaner.constructLocks(TABLENAME, kvs);
      Assert.fail();
    } catch (ThemisFatalException e) {}
  }
}