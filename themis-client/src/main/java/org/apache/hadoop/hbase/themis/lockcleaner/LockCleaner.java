package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.ServerLockCleaner;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpointClient;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LockCleaner extends ServerLockCleaner {
  private static final Logger LOG = LoggerFactory.getLogger(LockCleaner.class);
  private static Object cleanerLock = new Object();
  private static LockCleaner lockCleaner;
  private WorkerRegister register;
  private final int retry;
  private final int pause;
  private final int clientLockTTl;

  public static LockCleaner getLockCleaner(Configuration conf, Connection conn,
      WorkerRegister register, ThemisEndpointClient cpClient) throws IOException {
    if (lockCleaner == null) {
      synchronized (cleanerLock) {
        if (lockCleaner == null) {
          try {
            lockCleaner = new LockCleaner(conf, conn, register, cpClient);
          } catch (Exception e) {
            LOG.error("create LockCleaner fail", e);
            throw new IOException(e);
          }
        }
      }
    }
    return lockCleaner;
  }

  public LockCleaner(Configuration conf, Connection conn, WorkerRegister register,
      ThemisEndpointClient cpClient) throws IOException {
    super(conn, cpClient);
    this.register = register;
    retry = conf.getInt(TransactionConstant.THEMIS_RETRY_COUNT,
      TransactionConstant.DEFAULT_THEMIS_RETRY_COUNT);
    pause = conf.getInt(TransactionConstant.THEMIS_PAUSE, TransactionConstant.DEFAULT_THEMIS_PAUSE);
    clientLockTTl = conf.getInt(TransactionConstant.CLIENT_LOCK_TTL_KEY,
      TransactionConstant.DEFAULT_CLIENT_LOCK_TTL);
  }

  public boolean shouldCleanLock(ThemisLock lock) throws IOException {
    boolean isWorkerAlive = register.isWorkerAlive(lock.getClientAddress());
    if (!isWorkerAlive) {
      LOG.warn("worker, address=" + lock.getClientAddress() + " is not alive");
      return true;
    }
    return lock.isLockExpired();
  }

  public static List<ThemisLock> constructLocks(TableName tableName, List<Cell> lockKvs,
      ThemisEndpointClient cpClient, int clientLockTTL) throws IOException {
    List<ThemisLock> locks = new ArrayList<ThemisLock>();
    if (lockKvs != null) {
      for (Cell kv : lockKvs) {
        ColumnCoordinate lockColumn = new ColumnCoordinate(tableName, CellUtil.cloneRow(kv),
          CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv));
        if (!ColumnUtil.isLockColumn(lockColumn)) {
          throw new ThemisFatalException("get no-lock column when constructLocks, kv=" + kv);
        }
        ThemisLock lock =
          ThemisLock.parseFromBytes(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
        ColumnCoordinate dataColumn = new ColumnCoordinate(tableName, lockColumn.getRow(),
          ColumnUtil.getDataColumn(lockColumn));
        lock.setColumn(dataColumn);
        checkLockExpired(lock, cpClient, clientLockTTL);
        locks.add(lock);
      }
    }
    return locks;
  }

  public static void checkLockExpired(ThemisLock lock, ThemisEndpointClient cpClient,
      int clientLockTTL) throws IOException {
    if (clientLockTTL == 0) {
      // TODO : get lock expired in server side for the first time to check ttl
      lock.setLockExpired(cpClient.isLockExpired(lock.getColumn().getTableName(),
        lock.getColumn().getRow(), lock.getTimestamp()));
    } else {
      lock.setLockExpired(TransactionTTL.isLockExpired(lock.getTimestamp(), clientLockTTL));
      LOG.info("check lock expired by client lock ttl, lock=" + lock + ", clientLockTTL=" +
        clientLockTTL + ", type=" + TransactionTTL.timestampType);
    }
  }

  public void tryToCleanLocks(TableName tableName, List<Cell> lockColumns) throws IOException {
    List<ThemisLock> locks = constructLocks(tableName, lockColumns, cpClient, clientLockTTl);
    long startTs = lockColumns.get(0).getTimestamp();
    for (ThemisLock lock : locks) {
      if (tryToCleanLock(lock)) {
        LOG.warn("lock cleaned sucessfully, cleanTranStartTs=" + startTs + ", cleanFromLock=" +
          lock + ", prewriteTs=" + lock.getTimestamp());
      }
    }
  }

  public boolean checkLockExpiredAndTryToCleanLock(ThemisLock lock) throws IOException {
    checkLockExpired(lock, cpClient, clientLockTTl);
    return tryToCleanLock(lock);
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
        LOG.warn("sleep " + pause + " to clean lock, current=" + current + ", retry=" + retry +
          ", clientLockTTL=" + clientLockTTl + ", type=" + TransactionTTL.timestampType);
        Threads.sleep(pause);
        // must check expired between retry
        checkLockExpired(lock, cpClient, clientLockTTl);
      }
    }
    // check the whether the lock has been cleaned by other transaction
    if (hasLock(lock)) {
      throw new LockConflictException("lock can't be cleaned after retry=" + retry + ", column=" +
        lock.getColumn() + ", prewriteTs=" + lock.getTimestamp());
    } else {
      LOG.warn("lock has been clean by other transaction, lock=" + lock);
    }
    return false;
  }

  @VisibleForTesting
  boolean hasLock(ThemisLock lock) throws IOException {
    ColumnCoordinate columnCoordinate = lock.getColumn();
    Column lc = ColumnUtil.getLockColumn(columnCoordinate);
    try (Table table = conn.getTable(columnCoordinate.getTableName())) {
      Get get = new Get(columnCoordinate.getRow()).addColumn(lc.getFamily(), lc.getQualifier())
        .setTimestamp(lock.getTimestamp());
      return table.exists(get);
    }
  }
}
