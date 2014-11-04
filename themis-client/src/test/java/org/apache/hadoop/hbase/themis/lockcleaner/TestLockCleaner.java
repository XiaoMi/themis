package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.ThemisCoprocessorClient;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

public class TestLockCleaner extends ClientTestBase {
  protected ThemisCoprocessorClient cpClient;
  protected LockCleaner lockCleaner;
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    setConfigForLockCleaner(conf);
    cpClient = new ThemisCoprocessorClient(connection);
    lockCleaner = new LockCleaner(conf, connection, mockRegister, cpClient);
  }
  
  public static void setConfigForLockCleaner(Configuration conf) {
    conf.setInt(TransactionConstant.THEMIS_RETRY_COUNT, 2);    
  }

  @Test
  public void testShouldCleanLock() throws IOException {
    ThemisLock lock = getLock(COLUMN);
    Mockito.when(mockRegister.isWorkerAlive(lock.getClientAddress())).thenReturn(true);
    Assert.assertFalse(lockCleaner.shouldCleanLock(lock));
    Mockito.when(mockRegister.isWorkerAlive(lock.getClientAddress())).thenReturn(false);
    Assert.assertTrue(lockCleaner.shouldCleanLock(lock));
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
      // lock won't be cleaned by this transaction, however, cleaned by another transaction
      deleteOldDataAndUpdateTs();
      lc = getLock(COLUMN);
      Mockito.when(mockRegister.isWorkerAlive(lc.getClientAddress())).thenReturn(true);
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
        Assert.assertTrue(e.getCause() instanceof RetriesExhaustedException);
        admin.enableTable(TABLENAME);
        checkPrewriteColumnSuccess(COLUMN);
      } finally {
        admin.close();
      }
      // lock should not be cleaned after retry
      deleteOldDataAndUpdateTs();
      writeLockAndData(COLUMN);
      lc = getLock(COLUMN);
      Mockito.when(mockRegister.isWorkerAlive(lc.getClientAddress())).thenReturn(true);
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
  public void testLockCleanByClientTTl() throws IOException {
    boolean[] cleanLocksOptions = new boolean[]{false, true};
    for (boolean cleanLocks : cleanLocksOptions) {
      Configuration testConf = HBaseConfiguration.create(conf);
      // lock should be cleaned but clean fail
      ThemisLock lc = getLock(COLUMN);
      writeLockAndData(COLUMN);
      Mockito.when(mockRegister.isWorkerAlive(lc.getClientAddress())).thenReturn(true);

      // lock could be cleaned because client lock ttl is set
      // make sure the lock is expired by client ttl
      Assert.assertTrue(System.currentTimeMillis() - lc.getTimestamp() > 1);
      testConf.set(TransactionConstant.CLIENT_LOCK_TTL_KEY, String.valueOf("1"));
      testConf.setInt(TransactionConstant.THEMIS_PAUSE, 500);
      lockCleaner = new LockCleaner(testConf, connection, mockRegister, cpClient);
      if (!cleanLocks) {
        LockCleaner.checkLockExpired(lc, cpClient, 1);
      }
      long startTs = System.currentTimeMillis();
      invokeTryToCleanLock(lc, cleanLocks);
      checkColumnRollback(COLUMN);
      Assert.assertTrue(System.currentTimeMillis() - startTs < 500);
      
      // lock should be cleaned after retry
      lc.setTimestamp(System.currentTimeMillis());
      testConf.set(TransactionConstant.CLIENT_LOCK_TTL_KEY, String.valueOf("500"));
      lockCleaner = new LockCleaner(testConf, connection, mockRegister, cpClient);
      if (!cleanLocks) {
        LockCleaner.checkLockExpired(lc, cpClient, 500);
      }
      startTs = System.currentTimeMillis();
      invokeTryToCleanLock(lc, cleanLocks);
      checkColumnRollback(COLUMN);
      Assert.assertTrue(System.currentTimeMillis() - startTs >= 500);
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
    String rawClientLockTTL = conf.get(TransactionConstant.CLIENT_LOCK_TTL_KEY);
    int clientLockTTL = 10;
    Threads.sleep(clientLockTTL);
    Assert.assertTrue(System.currentTimeMillis() - expectLock.getTimestamp() > clientLockTTL);
    List<ThemisLock> locks = LockCleaner.constructLocks(TABLENAME, kvs, cpClient, clientLockTTL);
    Assert.assertEquals(1, locks.size());
    Assert.assertTrue(expectLock.equals(locks.get(0)));
    Assert.assertTrue(COLUMN.equals(locks.get(0).getColumn()));
    Assert.assertTrue(locks.get(0).isLockExpired());
    conf.set(TransactionConstant.CLIENT_LOCK_TTL_KEY, rawClientLockTTL != null ? rawClientLockTTL
        : "0");
    
    // with no-lock column, should throw exception
    kvs.add(KEYVALUE);
    try {
      LockCleaner.constructLocks(TABLENAME, kvs, cpClient, 0);
      Assert.fail();
    } catch (ThemisFatalException e) {}
  }  
}