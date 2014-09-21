package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.ServerLockCleaner;
import org.apache.hadoop.hbase.themis.cp.ThemisCoprocessorClient;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Threads;

public class LockCleaner extends ServerLockCleaner {
  private static final Log LOG = LogFactory.getLog(LockCleaner.class);
  private static Object cleanerLock = new Object();
  private static LockCleaner lockCleaner;
  private WorkerRegister register;
  private final int retry;
  private final int pause;
  
  public static LockCleaner getLockCleaner(Configuration conf, HConnection conn,
      WorkerRegister register, ThemisCoprocessorClient cpClient) throws IOException {
    if (lockCleaner == null) {
      synchronized (cleanerLock) {
        if (lockCleaner == null) {
          try {
            lockCleaner = new LockCleaner(conf, conn, register, cpClient);
          } catch (Exception e) {
            LOG.fatal("create LockCleaner fail", e);
            throw new IOException(e);
          }
        }
      }
    }
    return lockCleaner;
  }
  
  public LockCleaner(Configuration conf, HConnection conn, WorkerRegister register,
      ThemisCoprocessorClient cpClient) throws IOException {
    super(conn, cpClient);
    this.register = register;
    retry = conf.getInt(TransactionConstant.THEMIS_RETRY_COUNT,
      TransactionConstant.DEFAULT_THEMIS_RETRY_COUNT);
    pause = conf.getInt(TransactionConstant.THEMIS_PAUSE,
      TransactionConstant.DEFAULT_THEMIS_PAUSE);
  }
  
  public boolean shouldCleanLock(ThemisLock lock) throws IOException {
    boolean isWorkerAlive = register.isWorkerAlive(lock.getClientAddress());
    if (!isWorkerAlive) {
      LOG.warn("worker, address=" + lock.getClientAddress() + " is not alive");
      return true;
    }
    return lock.isLockExpired();
  }
  
  public static List<ThemisLock> constructLocks(byte[] tableName,
      List<KeyValue> lockKvs, ThemisCoprocessorClient cpClient) throws IOException {
    List<ThemisLock> locks = new ArrayList<ThemisLock>();
    if (lockKvs != null) {
      for (KeyValue kv : lockKvs) {
        ColumnCoordinate lockColumn = new ColumnCoordinate(tableName, kv.getRow(),
            kv.getFamily(), kv.getQualifier());
        if (!ColumnUtil.isLockColumn(lockColumn)) {
          throw new ThemisFatalException("get no-lock column when constructLocks, kv=" + kv);
        }
        ThemisLock lock = ThemisLock.parseFromByte(kv.getValue());
        ColumnCoordinate dataColumn = new ColumnCoordinate(tableName, lockColumn.getRow(),
            ColumnUtil.getDataColumn(lockColumn));
        lock.setColumn(dataColumn);
        // TODO : get lock expired in server side when get return
        lock.setLockExpired(cpClient.isLockExpired(tableName, kv.getRow(), kv.getTimestamp()));
        locks.add(lock);
      }
    }
    return locks;
  }
  
  public void tryToCleanLocks(byte[] tableName, List<KeyValue> lockColumns) throws IOException {
    List<ThemisLock> locks = constructLocks(tableName, lockColumns, cpClient);
    long startTs = lockColumns.get(0).getTimestamp();
    for (ThemisLock lock : locks) {
      if (tryToCleanLock(lock)) {
        LOG.warn("lock cleaned sucessfully, cleanTranStartTs=" + startTs + ", cleanFromLock="
            + lock + ", prewriteTs=" + lock.getTimestamp());
      }
    }
  }
  
  // try to clean lock, throw LockConflictException if the lock can't be cleaned after retry
  // return true if the lock is cleaned by this client; otherwise, return false;
  public boolean tryToCleanLock(ThemisLock lock) throws IOException {
    if (lock.getColumn() == null) {
      throw new ThemisFatalException("column of lock should not be null for clean, lock=" + lock);
    }
    for (int current = 0; current < retry; ++current) {
      if (this.shouldCleanLock(lock)) {
        this.cleanLock(lock);
        return true;
      }
      if (current + 1 < retry) {
        LOG.warn("sleep " + pause + " to clean lock, current=" + current + ", retry=" + retry);
        Threads.sleep(pause);
      }
    }
    // check the whether the lock has been cleaned by other transaction
    if (hasLock(lock)) {
      throw new LockConflictException("lock can't be cleaned after retry=" + retry + ", column="
          + lock.getColumn() + ", prewriteTs=" + lock.getTimestamp());
    } else {
      LOG.warn("lock has been clean by other transaction, lock=" + lock);
    }
    return false;
  }
  
  protected boolean hasLock(ThemisLock lock) throws IOException {
    ColumnCoordinate columnCoordinate = lock.getColumn();
    Column lc = ColumnUtil.getLockColumn(columnCoordinate);
    HTableInterface table = null;
    try {
      table = conn.getTable(columnCoordinate.getTableName());
      Get get = new Get(columnCoordinate.getRow()).addColumn(lc.getFamily(), lc.getQualifier());
      get.setTimeStamp(lock.getTimestamp());
      return !table.get(get).isEmpty();
    } finally {
      closeTable(table);
    }
  }  
}
