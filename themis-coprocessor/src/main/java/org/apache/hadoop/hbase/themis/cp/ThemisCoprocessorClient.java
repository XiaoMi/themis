package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.apache.hadoop.hbase.themis.exception.WriteConflictException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;

// coprocessor client for ThemisProtocol
public class ThemisCoprocessorClient {
  private final HConnection conn;

  public ThemisCoprocessorClient(HConnection connection) {
    this.conn = connection;
  }

  static class ResultCallback<R> implements Batch.Callback<R> {
    R result = null;
    public void update(byte[] region, byte[] row, R result) {
      this.result = result;
    }
  }
  
  static abstract class CoprocessorCallable<R> {
    private HConnection conn;
    private byte[] tableName;
    private HTableInterface table = null;
    
    public CoprocessorCallable(HConnection conn, byte[] tableName) {
      this.conn = conn;
      this.tableName = tableName;
    }

    public R run() throws IOException {
      try {
        table = conn.getTable(tableName);
        ResultCallback<R> callback = new ResultCallback<R>();
        invokeCoprocessor(table, callback);
        return callback.result;
      } catch (Throwable e) {
        throw new IOException(e);
      } finally {
        if (table != null) {
          table.close();
        }
      }
    }
    
    public abstract void invokeCoprocessor(HTableInterface table, ResultCallback<R> callback)
        throws Throwable;
  }
  
  public Result themisGet(final byte[] tableName, final Get get, final long startTs)
      throws IOException {
    return themisGet(tableName, get, startTs, false);
  }
  
  public Result themisGet(final byte[] tableName, final Get get, final long startTs,
      final boolean ignoreLock) throws IOException {
    return new CoprocessorCallable<Result>(conn, tableName) {
      @Override
      public void invokeCoprocessor(HTableInterface table, ResultCallback<Result> callback)
          throws Throwable {
        table.coprocessorExec(ThemisProtocol.class, get.getRow(), get.getRow(),
          new Batch.Call<ThemisProtocol, Result>() {
            public Result call(ThemisProtocol instance) throws IOException {
                return instance.themisGet(get, startTs, ignoreLock);
            }
          }, callback);
      }
    }.run();
  }
  
  public ThemisLock prewriteSecondaryRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] secondaryLock)
      throws IOException {
    return prewriteRow(tableName, row, mutations, prewriteTs, null, secondaryLock, -1);
  }
  
  public ThemisLock prewriteRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
      final byte[] secondaryLock, final int primaryIndex) throws IOException {
    CoprocessorCallable<byte[][]> callable = new CoprocessorCallable<byte[][]>(conn, tableName) {
      @Override
      public void invokeCoprocessor(HTableInterface table, ResultCallback<byte[][]> callback)
          throws Throwable {
        table.coprocessorExec(ThemisProtocol.class, row, row,
          new Batch.Call<ThemisProtocol, byte[][]>() {
            public byte[][] call(ThemisProtocol instance) throws IOException {
              return instance.prewriteRow(row, mutations, prewriteTs, secondaryLock, primaryLock,
                primaryIndex);
            }
          }, callback);
      }
    };
    return judgePerwriteResultRow(tableName, row, callable.run(), prewriteTs);
  }
  
  public ThemisLock prewriteSingleRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
      final byte[] secondaryLock, final int primaryIndex) throws IOException {
    CoprocessorCallable<byte[][]> callable = new CoprocessorCallable<byte[][]>(conn, tableName) {
      @Override
      public void invokeCoprocessor(HTableInterface table, ResultCallback<byte[][]> callback)
          throws Throwable {
        table.coprocessorExec(ThemisProtocol.class, row, row,
          new Batch.Call<ThemisProtocol, byte[][]>() {
            public byte[][] call(ThemisProtocol instance) throws IOException {
              return instance.prewriteSingleRow(row, mutations, prewriteTs, secondaryLock, primaryLock,
                primaryIndex);
            }
          }, callback);
      }
    };
    return judgePerwriteResultRow(tableName, row, callable.run(), prewriteTs);
  }
  
  protected ThemisLock judgePerwriteResultRow(byte[] tableName, byte[] row,
      byte[][] prewriteResult, long prewriteTs) throws IOException {
    if (prewriteResult != null) {
      long commitTs = Bytes.toLong(prewriteResult[0]);
      if (commitTs != 0) {
        throw new WriteConflictException("encounter write with larger timestamp than prewriteTs="
            + prewriteTs + ", commitTs=" + commitTs);
      } else {
        ThemisLock lock = ThemisLock.parseFromByte(prewriteResult[1]);
        ColumnCoordinate column = new ColumnCoordinate(tableName, row, prewriteResult[2], prewriteResult[3]);
        lock.setColumn(column);
        return lock;
      }
    }
    return null;
  }
  
  public void commitSecondaryRow(final byte[] tableName, final byte[] row,
      List<ColumnMutation> mutations, final long prewriteTs, final long commitTs)
      throws IOException {
    commitRow(tableName, row, mutations, prewriteTs, commitTs, -1);
  }
  
  public void commitRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final long commitTs,
      final int primaryIndex) throws IOException {
    CoprocessorCallable<Boolean> callable = new CoprocessorCallable<Boolean>(conn, tableName) {
      @Override
      public void invokeCoprocessor(HTableInterface table, ResultCallback<Boolean> callback)
          throws Throwable {
        table.coprocessorExec(ThemisProtocol.class, row, row,
          new Batch.Call<ThemisProtocol, Boolean>() {
            public Boolean call(ThemisProtocol instance) throws IOException {
              return instance.commitRow(row, mutations, prewriteTs, commitTs, primaryIndex);
            }
          }, callback);
      }
    };
    if (!callable.run()) {
      if (primaryIndex < 0) {
        throw new IOException("secondary row commit fail, should not happend!");
      } else {
        ColumnMutation primaryMutation = mutations.get(primaryIndex);
        throw new LockCleanedException("lock has been cleaned, column="
            + new ColumnCoordinate(tableName, row, primaryMutation.getFamily(), primaryMutation.getQualifier())
            + ", prewriteTs=" + prewriteTs);
      }
    }
  }
  
  public void commitSingleRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final long commitTs,
      final int primaryIndex) throws IOException {
    CoprocessorCallable<Boolean> callable = new CoprocessorCallable<Boolean>(conn, tableName) {
      @Override
      public void invokeCoprocessor(HTableInterface table, ResultCallback<Boolean> callback)
          throws Throwable {
        table.coprocessorExec(ThemisProtocol.class, row, row,
          new Batch.Call<ThemisProtocol, Boolean>() {
            public Boolean call(ThemisProtocol instance) throws IOException {
              return instance.commitSingleRow(row, mutations, prewriteTs, commitTs, primaryIndex);
            }
          }, callback);
      }
    };
    if (!callable.run()) {
      if (primaryIndex < 0) {
        throw new IOException("secondary row commit fail, should not happend!");
      } else {
        ColumnMutation primaryMutation = mutations.get(primaryIndex);
        throw new LockCleanedException("lock has been cleaned, column="
            + new ColumnCoordinate(tableName, row, primaryMutation.getFamily(), primaryMutation.getQualifier())
            + ", prewriteTs=" + prewriteTs);
      }
    }
  }
  
  public ThemisLock getLockAndErase(final ColumnCoordinate columnCoordinate, final long prewriteTs)
      throws IOException {
    CoprocessorCallable<byte[]> callable = new CoprocessorCallable<byte[]>(conn, columnCoordinate.getTableName()) {
      @Override
      public void invokeCoprocessor(HTableInterface table, ResultCallback<byte[]> callback)
          throws Throwable {
        table.coprocessorExec(ThemisProtocol.class, columnCoordinate.getRow(), columnCoordinate.getRow(),
          new Batch.Call<ThemisProtocol, byte[]>() {
            public byte[] call(ThemisProtocol instance) throws IOException {
              return instance.getLockAndErase(columnCoordinate.getRow(), columnCoordinate.getFamily(),
                columnCoordinate.getQualifier(), prewriteTs);
            }
          }, callback);
      }
    };
    byte[] result = callable.run();
    return result == null ? null : ThemisLock.parseFromByte(result);
  }
}