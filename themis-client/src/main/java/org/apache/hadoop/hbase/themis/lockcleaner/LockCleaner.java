package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.ThemisStatistics;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.ThemisCoprocessorClient;
import org.apache.hadoop.hbase.themis.cp.ThemisCpUtil;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.themis.lock.PrimaryLock;
import org.apache.hadoop.hbase.themis.lock.SecondaryLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.collect.Lists;

public class LockCleaner {
  private static final Log LOG = LogFactory.getLog(LockCleaner.class);
  private WorkerRegister register;
  private WallClock wallClock;
  private int lockTTL;
  private HConnection conn;
  private int retry;
  private int pause;
  private final ThemisCoprocessorClient cpClient;
  
  public LockCleaner(Configuration conf, HConnection conn, WallClock wallClock,
      WorkerRegister register, ThemisCoprocessorClient cpClient) throws IOException {
    this.conn = conn;
    this.wallClock = wallClock;
    this.register = register;
    this.cpClient = cpClient;
    lockTTL = conf.getInt(TransactionConstant.THEMIS_LOCK_TTL_KEY,
      TransactionConstant.DEFAULT_THEMIS_LOCK_TTL);
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
    return isLockExpired(lock);
  }
  
  public boolean isLockExpired(ThemisLock lock) {
    return lock.getWallTime() + lockTTL <= wallClock.getWallTime();
  }

  // get primary column coordinate from conflict lock
  protected static PrimaryLock getPrimaryLockWithColumn(ThemisLock lock) {
    if (lock.isPrimary()) {
      return (PrimaryLock)lock;
    } else {
      PrimaryLock primaryLock = new PrimaryLock();
      primaryLock.setColumn(((SecondaryLock)lock).getPrimaryColumn());
      ThemisLock.copyThemisLock(lock, primaryLock);
      primaryLock.addSecondaryColumn(lock.getColumn(), lock.getType());
      return primaryLock;
    }
  }
  
  // clean the lock either by erase the lock or commit the transaction
  public void cleanLock(ThemisLock lock) throws IOException {
    long beginTs = System.nanoTime();
    try {
      PrimaryLock primary = getPrimaryLockWithColumn(lock);
      // judge whether the transaction committed by clean the primary lock
      Pair<Long, PrimaryLock> cleanResult = cleanPrimaryLock(primary.getColumn(), primary.getTimestamp());
      // if the primary lock is cleaned in cleanPrimaryLock, we could get the primary lock with all secondaries
      primary = cleanResult.getSecond() == null ? primary : cleanResult.getSecond();
      // clean secondary locks
      cleanSecondaryLocks(primary, cleanResult.getFirst());
      ThemisStatistics.getStatistics().cleanLockSuccessCount.inc();
    } catch (IOException e) {
      ThemisStatistics.getStatistics().cleanLockFailCount.inc();
      throw e;
    } finally {
      ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().cleanLockLatency, beginTs);
    }
  }
  
  // clean primary lock erasing lock. return commitTs if the primary has been committed by other client;
  // otherwise, return the cleaned primary lock if the lock is cleaned by this client.
  protected Pair<Long, PrimaryLock> cleanPrimaryLock(ColumnCoordinate columnCoordinate,
      long prewriteTs) throws IOException {
    // read back and erase primary lock if exist
    ThemisLock lock = cpClient.getLockAndErase(columnCoordinate, prewriteTs);
    // make sure we must get a primary lock when there are logic errors in code
    if (lock != null && !(lock instanceof PrimaryLock)) {
      throw new ThemisFatalException("encounter no-primary lock when cleanPrimaryLock, column="
          + columnCoordinate + ", prewriteTs=" + prewriteTs + ", lock=" + lock);
    }
    PrimaryLock primaryLock = (PrimaryLock)lock;
    Long commitTs = primaryLock != null ? null :
      getTimestampOfWriteIndexingPrewriteTs(columnCoordinate, prewriteTs);
    // commitTs = null indicates the conflicted transaction has been erased by other client; otherwise
    // the conflicted must be committed by other client.
    if (commitTs == null) {
      ThemisStatistics.getStatistics().cleanLockByEraseCount.inc();
    } else {
      ThemisStatistics.getStatistics().cleanLockByCommitCount.inc();
    }
    return new Pair<Long, PrimaryLock>(commitTs, primaryLock);
  }
  
  // get the timestamp of write-column kv which has value equal to 'timestamp'
  public Long getTimestampOfWriteIndexingPrewriteTs(ColumnCoordinate columnCoordinate, long timestamp)
      throws IOException {
    HTableInterface table = null;
    try {
      table = conn.getTable(columnCoordinate.getTableName());
      Get get = createGetOfWriteColumnsIndexingPrewriteTs(columnCoordinate, timestamp);
      Result result = conn.getTable(columnCoordinate.getTableName()).get(get);
      if (result.list() != null) {
        for (KeyValue kv : result.list()) {
          long prewriteTs = Bytes.toLong(kv.getValue());
          if (prewriteTs == timestamp) {
            return kv.getTimestamp();
          }
        }
      }
      return null;
    } finally {
      closeTable(table);
    }
  }
  
  protected Get createGetOfWriteColumnsIndexingPrewriteTs(ColumnCoordinate columnCoordinate, long timestamp)
      throws IOException {
    Get get = new Get(columnCoordinate.getRow());
    ThemisCpUtil.addWriteColumnToGet(columnCoordinate, get);
    get.setTimeRange(timestamp, Long.MAX_VALUE);
    get.setMaxVersions();
    return get;
  }
  
  // erase lock and data if commitTs is null; otherwise, commit it.
  protected void cleanSecondaryLocks(PrimaryLock primaryLock, Long commitTs)
      throws IOException {
    for (Entry<ColumnCoordinate, Type> secondary : primaryLock.getSecondaryColumns().entrySet()) {
      if (commitTs == null) {
        eraseLockAndData(secondary.getKey(), primaryLock.getTimestamp());
      } else {
        ColumnCoordinate column = secondary.getKey();
        ColumnMutation columnMutation = new ColumnMutation(column, secondary.getValue(), null);
        cpClient.commitSecondaryRow(column.getTableName(), column.getRow(),
          Lists.newArrayList(columnMutation), primaryLock.getTimestamp(), commitTs);
      }
    }
  }

  public void eraseLockAndData(ColumnCoordinate column, long timestamp) throws IOException {
    eraseLockAndData(column.getTableName(), column.getRow(), Lists.<Column>newArrayList(column), timestamp);
  }

  // erase the lock and data with given timestamp
  public void eraseLockAndData(byte[] tableName, byte[] row, Collection<Column> columns, long timestamp)
      throws IOException {
    HTableInterface table = null;
    try {
      table = conn.getTable(tableName);
      Delete delete = new Delete(row);
      for (Column column : columns) {
        Column lockColumn = ColumnUtil.getLockColumn(column);
        // delete data and lock column
        delete.deleteColumn(column.getFamily(), column.getQualifier(), timestamp);
        delete.deleteColumn(lockColumn.getFamily(), lockColumn.getQualifier(), timestamp);
      }
      table.delete(delete);
    } finally {
      closeTable(table);
    }
  }
  
  public static List<ThemisLock> constructLocks(byte[] tableName,
      List<KeyValue> lockKvs) throws IOException {
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
        locks.add(lock);
      }
    }
    return locks;
  }
  
  public void tryToCleanLocks(byte[] tableName, List<KeyValue> lockColumns) throws IOException {
    List<ThemisLock> locks = constructLocks(tableName, lockColumns);
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
  
  private void closeTable(HTableInterface table) throws IOException {
    if (table != null) {
      table.close();
    }
  }
}
