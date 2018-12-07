package org.apache.hadoop.hbase.themis.cp;

import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.EraseLockRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.EraseLockResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.LockExpiredRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.LockExpiredResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisCommitRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisCommitResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisGetRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisGetRequest.Builder;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisPrewriteRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisPrewriteResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisService.Stub;
import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.apache.hadoop.hbase.themis.exception.WriteConflictException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.themis.cp.BlockingRpcCallback;

import com.xiaomi.infra.thirdparty.com.google.common.base.Throwables;

// coprocessor client for ThemisProtocol
public class ThemisEndpointClient {
  private final Connection conn;

  public ThemisEndpointClient(Connection connection) {
    this.conn = connection;
  }

  static abstract class CoprocessorCallable<R> {
    private Connection conn;
    private TableName tableName;
    private byte[] row;

    public CoprocessorCallable(Connection conn, TableName tableName, byte[] row) {
      this.conn = conn;
      this.tableName = tableName;
      this.row = row;
    }

    public R run() throws IOException {
      try (Table table = conn.getTable(tableName)) {
        CoprocessorRpcChannel channel = table.coprocessorService(row);
        Stub stub = (Stub) ProtobufUtil.newServiceStub(ThemisProtos.ThemisService.class, channel);
        return invokeCoprocessor(stub);
      } catch (Throwable e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new IOException(e);
      }
    }

    public abstract R invokeCoprocessor(Stub stub) throws Throwable;
  }

  public Result themisGet(TableName tableName, Get get, long startTs) throws IOException {
    return themisGet(tableName, get, startTs, false);
  }

  protected static void checkRpcException(ServerRpcController controller) throws IOException {
    if (controller.getFailedOn() != null) {
      throw controller.getFailedOn();
    }
  }

  public Result themisGet(TableName tableName, final Get get, final long startTs,
      final boolean ignoreLock) throws IOException {
    return new CoprocessorCallable<Result>(conn, tableName, get.getRow()) {
      @Override
      public Result invokeCoprocessor(Stub instance) throws Throwable {
        Builder builder = ThemisGetRequest.newBuilder();
        builder.setGet(ProtobufUtil.toGet(get));
        builder.setStartTs(startTs);
        builder.setIgnoreLock(ignoreLock);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ClientProtos.Result> rpcCallback =
          new BlockingRpcCallback<ClientProtos.Result>();
        instance.themisGet(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return ProtobufUtil.toResult(rpcCallback.get());
      }
    }.run();
  }

  public ThemisLock prewriteSecondaryRow(TableName tableName, byte[] row,
      List<ColumnMutation> mutations, long prewriteTs, byte[] secondaryLock) throws IOException {
    return prewriteRow(tableName, row, mutations, prewriteTs, null, secondaryLock, -1);
  }

  public ThemisLock prewriteRow(TableName tableName, byte[] row, List<ColumnMutation> mutations,
      long prewriteTs, byte[] primaryLock, byte[] secondaryLock, int primaryIndex)
      throws IOException {
    return prewriteRow(tableName, row, mutations, prewriteTs, primaryLock, secondaryLock,
      primaryIndex, false);
  }

  public ThemisLock prewriteSingleRow(TableName tableName, byte[] row,
      List<ColumnMutation> mutations, long prewriteTs, byte[] primaryLock, byte[] secondaryLock,
      int primaryIndex) throws IOException {
    return prewriteRow(tableName, row, mutations, prewriteTs, primaryLock, secondaryLock,
      primaryIndex, true);
  }

  protected ThemisLock prewriteRow(TableName tableName, byte[] row, List<ColumnMutation> mutations,
      long prewriteTs, byte[] primaryLock, byte[] secondaryLock, int primaryIndex,
      boolean isSingleRow) throws IOException {
    CoprocessorCallable<byte[][]> callable =
      new CoprocessorCallable<byte[][]>(conn, tableName, row) {
        @Override
        public byte[][] invokeCoprocessor(Stub instance) throws Throwable {
          ThemisPrewriteRequest.Builder builder = ThemisPrewriteRequest.newBuilder();
          builder.setRow(HBaseZeroCopyByteString.wrap(row));
          for (ColumnMutation mutation : mutations) {
            builder.addMutations(ColumnMutation.toCell(mutation));
          }
          builder.setPrewriteTs(prewriteTs);
          builder.setPrimaryLock(HBaseZeroCopyByteString
            .wrap(primaryLock == null ? HConstants.EMPTY_BYTE_ARRAY : primaryLock));
          builder.setSecondaryLock(HBaseZeroCopyByteString
            .wrap(secondaryLock == null ? HConstants.EMPTY_BYTE_ARRAY : secondaryLock));
          builder.setPrimaryIndex(primaryIndex);
          ServerRpcController controller = new ServerRpcController();
          BlockingRpcCallback<ThemisPrewriteResponse> rpcCallback =
            new BlockingRpcCallback<ThemisPrewriteResponse>();
          if (isSingleRow) {
            instance.prewriteSingleRow(controller, builder.build(), rpcCallback);
          } else {
            instance.prewriteRow(controller, builder.build(), rpcCallback);
          }
          checkRpcException(controller);
          List<ByteString> pbResult = rpcCallback.get().getResultList();
          if (pbResult.size() == 0) {
            return null;
          } else {
            byte[][] results = new byte[pbResult.size()][];
            for (int i = 0; i < pbResult.size(); ++i) {
              results[i] = pbResult.get(i).toByteArray();
            }
            return results;
          }
        }
      };
    return judgePerwriteResultRow(tableName, row, callable.run(), prewriteTs);
  }

  protected ThemisLock judgePerwriteResultRow(TableName tableName, byte[] row,
      byte[][] prewriteResult, long prewriteTs) throws IOException {
    if (prewriteResult != null) {
      long commitTs = Bytes.toLong(prewriteResult[0]);
      if (commitTs != 0) {
        throw new WriteConflictException("encounter write with larger timestamp than prewriteTs=" +
          prewriteTs + ", commitTs=" + commitTs);
      } else {
        ThemisLock lock = ThemisLock.parseFromByte(prewriteResult[1]);
        ColumnCoordinate column =
          new ColumnCoordinate(tableName, row, prewriteResult[2], prewriteResult[3]);
        lock.setColumn(column);
        lock.setLockExpired(Bytes.toBoolean(prewriteResult[4]));
        return lock;
      }
    }
    return null;
  }

  public void commitSecondaryRow(TableName tableName, byte[] row, List<ColumnMutation> mutations,
      long prewriteTs, long commitTs) throws IOException {
    commitRow(tableName, row, mutations, prewriteTs, commitTs, -1);
  }

  public void commitRow(TableName tableName, byte[] row, List<ColumnMutation> mutations,
      long prewriteTs, long commitTs, int primaryIndex) throws IOException {
    CoprocessorCallable<Boolean> callable = new CoprocessorCallable<Boolean>(conn, tableName, row) {
      @Override
      public Boolean invokeCoprocessor(Stub instance) throws Throwable {
        ThemisCommitRequest.Builder builder = ThemisCommitRequest.newBuilder();
        builder.setRow(HBaseZeroCopyByteString.wrap(row));
        for (ColumnMutation mutation : mutations) {
          builder.addMutations(ColumnMutation.toCell(mutation));
        }
        builder.setPrewriteTs(prewriteTs);
        builder.setCommitTs(commitTs);
        builder.setPrimaryIndex(primaryIndex);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ThemisCommitResponse> rpcCallback =
          new BlockingRpcCallback<ThemisCommitResponse>();
        instance.commitRow(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().getResult();
      }
    };
    if (!callable.run()) {
      if (primaryIndex < 0) {
        throw new IOException("secondary row commit fail, should not happend!");
      } else {
        ColumnMutation primaryMutation = mutations.get(primaryIndex);
        throw new LockCleanedException("lock has been cleaned, column=" +
          new ColumnCoordinate(tableName, row, primaryMutation.getFamily(),
            primaryMutation.getQualifier()) +
          ", prewriteTs=" + prewriteTs);
      }
    }
  }

  public void commitSingleRow(TableName tableName, byte[] row, List<ColumnMutation> mutations,
      long prewriteTs, long commitTs, int primaryIndex) throws IOException {
    CoprocessorCallable<Boolean> callable = new CoprocessorCallable<Boolean>(conn, tableName, row) {
      @Override
      public Boolean invokeCoprocessor(Stub instance) throws Throwable {
        ThemisCommitRequest.Builder builder = ThemisCommitRequest.newBuilder();
        builder.setRow(HBaseZeroCopyByteString.wrap(row));
        for (ColumnMutation mutation : mutations) {
          builder.addMutations(ColumnMutation.toCell(mutation));
        }
        builder.setPrewriteTs(prewriteTs);
        builder.setCommitTs(commitTs);
        builder.setPrimaryIndex(primaryIndex);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ThemisCommitResponse> rpcCallback =
          new BlockingRpcCallback<ThemisCommitResponse>();
        instance.commitSingleRow(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().getResult();
      }
    };
    if (!callable.run()) {
      if (primaryIndex < 0) {
        throw new IOException("secondary row commit fail, should not happend!");
      } else {
        ColumnMutation primaryMutation = mutations.get(primaryIndex);
        throw new LockCleanedException("lock has been cleaned, column=" +
          new ColumnCoordinate(tableName, row, primaryMutation.getFamily(),
            primaryMutation.getQualifier()) +
          ", prewriteTs=" + prewriteTs);
      }
    }
  }

  public ThemisLock getLockAndErase(ColumnCoordinate columnCoordinate, long prewriteTs)
      throws IOException {
    CoprocessorCallable<byte[]> callable = new CoprocessorCallable<byte[]>(conn,
      columnCoordinate.getTableName(), columnCoordinate.getRow()) {
      @Override
      public byte[] invokeCoprocessor(Stub instance) throws Throwable {
        EraseLockRequest.Builder builder = EraseLockRequest.newBuilder();
        builder.setRow(HBaseZeroCopyByteString.wrap(columnCoordinate.getRow()));
        builder.setFamily(HBaseZeroCopyByteString.wrap(columnCoordinate.getFamily()));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(columnCoordinate.getQualifier()));
        builder.setPrewriteTs(prewriteTs);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<EraseLockResponse> rpcCallback =
          new BlockingRpcCallback<EraseLockResponse>();
        instance.getLockAndErase(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().hasLock() ? rpcCallback.get().getLock().toByteArray() : null;
      }
    };
    byte[] result = callable.run();
    return result == null ? null : ThemisLock.parseFromByte(result);
  }

  public boolean isLockExpired(TableName tableName, byte[] row, long timestamp) throws IOException {
    return new CoprocessorCallable<Boolean>(conn, tableName, row) {
      @Override
      public Boolean invokeCoprocessor(Stub instance) throws Throwable {
        LockExpiredRequest.Builder builder = LockExpiredRequest.newBuilder();
        builder.setTimestamp(timestamp);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<LockExpiredResponse> rpcCallback =
          new BlockingRpcCallback<LockExpiredResponse>();
        instance.isLockExpired(controller, builder.build(), rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().getExpired();
      }
    }.run();
  }
}
