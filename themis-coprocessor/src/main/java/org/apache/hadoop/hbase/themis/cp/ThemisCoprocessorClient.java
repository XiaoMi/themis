package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
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

  static abstract class CoprocessorCallable<R> {
    private HConnection conn;
    private byte[] tableName;
    private byte[] row;
    private HTableInterface table = null;
    
    public CoprocessorCallable(HConnection conn, byte[] tableName, byte[] row) {
      this.conn = conn;
      this.tableName = tableName;
      this.row = row;
    }

    public R run() throws IOException {
      try {
        table = conn.getTable(tableName);
        ThemisProtocol themisProtocol = table.coprocessorProxy(ThemisProtocol.class, row);
        return invokeCoprocessor(themisProtocol);
      } catch (Throwable e) {
        throw new IOException(e);
      } finally {
        if (table != null) {
          table.close();
        }
      }
    }
    
    public abstract R invokeCoprocessor(ThemisProtocol themisProtocol) throws Throwable;
  }
  
  public Result themisGet(final byte[] tableName, final Get get, final long startTs)
      throws IOException {
    return themisGet(tableName, get, startTs, false);
  }
  
  protected static ThemisProtocol getThemisProtocol(HConnection conn, byte[] tableName, byte[] row)
      throws IOException {
    return conn.getTable(tableName).coprocessorProxy(ThemisProtocol.class, row);
  }
  
  public Result themisGet(final byte[] tableName, final Get get, final long startTs,
      final boolean ignoreLock) throws IOException {
    return new CoprocessorCallable<Result>(conn, tableName, get.getRow()) {
      @Override
      public Result invokeCoprocessor(ThemisProtocol instance) throws Throwable {
        return instance.themisGet(get, startTs, ignoreLock);
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
    CoprocessorCallable<byte[][]> callable = new CoprocessorCallable<byte[][]>(conn, tableName, row) {
      @Override
      public byte[][] invokeCoprocessor(ThemisProtocol instance) throws Throwable {
        return instance.prewriteRow(row, mutations, prewriteTs, secondaryLock, primaryLock,
          primaryIndex);
      }
    };
    return judgePerwriteResultRow(tableName, row, callable.run(), prewriteTs);
  }
  
  public ThemisLock prewriteSingleRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
      final byte[] secondaryLock, final int primaryIndex) throws IOException {
    CoprocessorCallable<byte[][]> callable = new CoprocessorCallable<byte[][]>(conn, tableName, row) {
      @Override
      public byte[][] invokeCoprocessor(ThemisProtocol instance) throws Throwable {
        return instance.prewriteSingleRow(row, mutations, prewriteTs, secondaryLock, primaryLock,
          primaryIndex);
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
    CoprocessorCallable<Boolean> callable = new CoprocessorCallable<Boolean>(conn, tableName, row) {
      @Override
      public Boolean invokeCoprocessor(ThemisProtocol instance) throws Throwable {
        return instance.commitRow(row, mutations, prewriteTs, commitTs, primaryIndex);
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
    CoprocessorCallable<Boolean> callable = new CoprocessorCallable<Boolean>(conn, tableName, row) {
      @Override
      public Boolean invokeCoprocessor(ThemisProtocol instance) throws Throwable {
        return instance.commitSingleRow(row, mutations, prewriteTs, commitTs, primaryIndex);
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
    CoprocessorCallable<byte[]> callable = new CoprocessorCallable<byte[]>(conn,
        columnCoordinate.getTableName(), columnCoordinate.getRow()) {
      @Override
      public byte[] invokeCoprocessor(ThemisProtocol instance) throws Throwable {
        return instance.getLockAndErase(columnCoordinate.getRow(), columnCoordinate.getFamily(),
          columnCoordinate.getQualifier(), prewriteTs);
      }
    };
    byte[] result = callable.run();
    return result == null ? null : ThemisLock.parseFromByte(result);
  }
}