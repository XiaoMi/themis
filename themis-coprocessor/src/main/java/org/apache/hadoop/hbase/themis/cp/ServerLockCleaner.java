package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.themis.lock.PrimaryLock;
import org.apache.hadoop.hbase.themis.lock.SecondaryLock;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;

public class ServerLockCleaner {
  protected ThemisEndpointClient cpClient;
  protected final HConnection conn;
  
  public ServerLockCleaner(HConnection conn, ThemisEndpointClient cpClient) throws IOException {
    this.conn = conn;
    this.cpClient = cpClient;
  }
  
  // get primary column coordinate from conflict lock
  public static PrimaryLock getPrimaryLockWithColumn(ThemisLock lock) {
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
  
  // TODO : add lock clean metrics in server side?
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
      ThemisCpStatistics.getThemisCpStatistics().cleanLockSuccessCount.inc();
    } catch (IOException e) {
      ThemisCpStatistics.getThemisCpStatistics().cleanLockFailCount.inc();
      throw e;
    } finally {
      ThemisCpStatistics.updateLatency(ThemisCpStatistics.getThemisCpStatistics().cleanLockLatency,
        beginTs, "lock=" + lock);
    }
  }
  
  // clean primary lock erasing lock. return commitTs if the primary has been committed by other client;
  // otherwise, return the cleaned primary lock if the lock is cleaned by this client.
  public Pair<Long, PrimaryLock> cleanPrimaryLock(ColumnCoordinate columnCoordinate,
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
      ThemisCpStatistics.getThemisCpStatistics().cleanLockByEraseCount.inc();
    } else {
      ThemisCpStatistics.getThemisCpStatistics().cleanLockByCommitCount.inc();
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
  
  public Get createGetOfWriteColumnsIndexingPrewriteTs(ColumnCoordinate columnCoordinate, long timestamp)
      throws IOException {
    Get get = new Get(columnCoordinate.getRow());
    ThemisCpUtil.addWriteColumnToGet(columnCoordinate, get);
    get.setTimeRange(timestamp, Long.MAX_VALUE);
    get.setMaxVersions();
    return get;
  }
  
  // erase lock and data if commitTs is null; otherwise, commit it.
  public void cleanSecondaryLocks(PrimaryLock primaryLock, Long commitTs)
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
        
        // Do not delete data when erasing lock. For replication, the lock and data of the column
        // maybe replicated firstly, then applications may try to clean the lock, if the data is
        // cleaned, there will be orphan commit column. There won't be much uncleanned data if we
        // assume the lock conflict situation won't happen frequently.
        // TODO: better way to handle the replication case?
        // delete.deleteColumn(column.getFamily(), column.getQualifier(), timestamp);
        delete.deleteColumn(lockColumn.getFamily(), lockColumn.getQualifier(), timestamp);
      }
      table.delete(delete);
    } finally {
      closeTable(table);
    }
  }
  
  protected void closeTable(HTableInterface table) throws IOException {
    if (table != null) {
      table.close();
    }
  }
}
