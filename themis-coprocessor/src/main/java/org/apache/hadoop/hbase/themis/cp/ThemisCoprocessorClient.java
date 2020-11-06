package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos;
import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.apache.hadoop.hbase.themis.exception.WriteConflictException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;

// coprocessor client for ThemisProtocol
public class ThemisCoprocessorClient {
  private final Connection conn;

  public ThemisCoprocessorClient(Connection connection) {
    this.conn = connection;
  }

  static abstract class CoprocessorCallable<R> {
    private Connection conn;
    private byte[] tableName;
    private byte[] row;
    private Table table = null;
    
    public CoprocessorCallable(Connection conn, byte[] tableName, byte[] row) {
      this.conn = conn;
      this.tableName = tableName;
      this.row = row;
    }

    public R run() throws IOException {
      try {
        final TableName tn = TableName.valueOf(Bytes.toString(tableName));
        table = conn.getTable(tn);

        CoprocessorRpcChannel coprocessorRpcChannel = table.coprocessorService(row);
        table.coprocessorService(row);
        ThemisProtos.ThemisService.Stub stub = (ThemisProtos.ThemisService.Stub) ProtobufUtil.newServiceStub(ThemisProtos.ThemisService.class, coprocessorRpcChannel);
        return invokeCoprocessor(stub);
      } catch (Throwable e) {
        throw new IOException(e);
      } finally {
        if (table != null) {
          table.close();
        }
      }
    }
    
    public abstract R invokeCoprocessor(ThemisProtos.ThemisService.Stub stub) throws Throwable;
  }

  protected static void checkRpcException(ServerRpcController controller) throws IOException {
    if (controller.getFailedOn() != null) {
      throw controller.getFailedOn();
    }
  }
  
  public Result themisGet(final byte[] tableName, final Get get, final long startTs)
      throws IOException {
    return themisGet(tableName, get, startTs, false);
  }
  
//  protected static ThemisProtocol getThemisProtocol(Connection conn, byte[] tableName, byte[] row)
//      throws IOException {
//    final TableName tn = TableName.valueOf(Bytes.toString(tableName));
//    return conn.getTable(tn).coprocessorProxy(ThemisProtocol.class, row);
//  }
  
  public Result themisGet(final byte[] tableName, final Get get, final long startTs,
      final boolean ignoreLock) throws IOException {
    return new CoprocessorCallable<Result>(conn, tableName, get.getRow()) {
      @Override
      public Result invokeCoprocessor(ThemisProtos.ThemisService.Stub instance) throws Throwable {
        ThemisProtos.ThemisGetRequest.Builder builder = ThemisProtos.ThemisGetRequest.newBuilder();
        builder.setGet(ProtobufUtil.toGet(get));
        builder.setStartTs(startTs);
        builder.setIgnoreLock(ignoreLock);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ClientProtos.Result> rpcCallback = new BlockingRpcCallback<>();
        instance.themisGet(controller, builder.build(),
                (com.google.protobuf.RpcCallback<ClientProtos.Result>) rpcCallback);
        checkRpcException(controller);
        return ProtobufUtil.toResult(rpcCallback.get());
      }
    }.run();
  }
  
  public ThemisLock prewriteSecondaryRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] secondaryLock)
      throws IOException {
    return prewriteRow(tableName, row, mutations, prewriteTs, null, secondaryLock, -1, false);
  }

  public ThemisLock prewriteRow(final byte[] tableName, final byte[] row,
                                  final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
                                  final byte[] secondaryLock, final int primaryIndex) throws IOException {
    return prewriteRow(tableName, row, mutations, prewriteTs, primaryLock, secondaryLock, primaryIndex, false);
  }
  
  public ThemisLock prewriteRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
      final byte[] secondaryLock, final int primaryIndex, final boolean isSingleRow) throws IOException {
    CoprocessorCallable<byte[][]> callable = new CoprocessorCallable<byte[][]>(conn, tableName, row) {
      @Override
      public byte[][] invokeCoprocessor(ThemisProtos.ThemisService.Stub instance) throws Throwable {
        ThemisProtos.ThemisPrewriteRequest.Builder builder = ThemisProtos.ThemisPrewriteRequest.newBuilder();
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
        BlockingRpcCallback<ThemisProtos.ThemisPrewriteResponse> rpcCallback = new BlockingRpcCallback<>();
        if (isSingleRow) {
          instance.prewriteSingleRow(controller, builder.build(),
                  (com.google.protobuf.RpcCallback<ThemisProtos.ThemisPrewriteResponse>) rpcCallback);
        } else {
          instance.prewriteRow(controller, builder.build(),
                  (com.google.protobuf.RpcCallback<ThemisProtos.ThemisPrewriteResponse>) rpcCallback);
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


    public ThemisLock prewriteSingleRow(final byte[] tableName, final byte[] row,
                                    final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
                                    final byte[] secondaryLock, final int primaryIndex) throws IOException {
      return prewriteRow(tableName, row, mutations, prewriteTs, primaryLock, secondaryLock, primaryIndex, true);
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
        lock.setLockExpired(Bytes.toBoolean(prewriteResult[4]));
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
      public Boolean invokeCoprocessor(ThemisProtos.ThemisService.Stub instance) throws Throwable {
        ThemisProtos.ThemisCommitRequest.Builder builder = ThemisProtos.ThemisCommitRequest.newBuilder();
        builder.setRow(HBaseZeroCopyByteString.wrap(row));
        for (ColumnMutation mutation : mutations) {
          builder.addMutations(ColumnMutation.toCell(mutation));
        }
        builder.setPrewriteTs(prewriteTs);
        builder.setCommitTs(commitTs);
        builder.setPrimaryIndex(primaryIndex);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ThemisProtos.ThemisCommitResponse> rpcCallback = new BlockingRpcCallback<>();
        instance.commitRow(controller, builder.build(),
                (com.google.protobuf.RpcCallback<ThemisProtos.ThemisCommitResponse>) rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().getResult();
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
      public Boolean invokeCoprocessor(ThemisProtos.ThemisService.Stub instance) throws Throwable {
        ThemisProtos.ThemisCommitRequest.Builder builder = ThemisProtos.ThemisCommitRequest.newBuilder();
        builder.setRow(HBaseZeroCopyByteString.wrap(row));
        for (ColumnMutation mutation : mutations) {
          builder.addMutations(ColumnMutation.toCell(mutation));
        }
        builder.setPrewriteTs(prewriteTs);
        builder.setCommitTs(commitTs);
        builder.setPrimaryIndex(primaryIndex);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ThemisProtos.ThemisCommitResponse> rpcCallback = new BlockingRpcCallback<ThemisProtos.ThemisCommitResponse>();
        instance.commitSingleRow(controller, builder.build(),
              (com.google.protobuf.RpcCallback<ThemisProtos.ThemisCommitResponse>) rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().getResult();
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
      public byte[] invokeCoprocessor(ThemisProtos.ThemisService.Stub instance) throws Throwable {
        ThemisProtos.EraseLockRequest.Builder builder = ThemisProtos.EraseLockRequest.newBuilder();
          builder.setRow(HBaseZeroCopyByteString.wrap(columnCoordinate.getRow()));
          builder.setFamily(HBaseZeroCopyByteString.wrap(columnCoordinate.getFamily()));
          builder.setQualifier(HBaseZeroCopyByteString.wrap(columnCoordinate.getQualifier()));
          builder.setPrewriteTs(prewriteTs);
          ServerRpcController controller = new ServerRpcController();
          BlockingRpcCallback<ThemisProtos.EraseLockResponse> rpcCallback = new BlockingRpcCallback<>();
          instance.getLockAndErase(controller, builder.build(),
                  (com.google.protobuf.RpcCallback<ThemisProtos.EraseLockResponse>) rpcCallback);
          checkRpcException(controller);
          return rpcCallback.get().hasLock() ? rpcCallback.get().getLock().toByteArray() : null;
      }
    };
    byte[] result = callable.run();
    return result == null ? null : ThemisLock.parseFromByte(result);
  }
  
  public boolean isLockExpired(final byte[] tableName, final byte[] row, final long timestamp)
      throws IOException {
    return new CoprocessorCallable<Boolean>(conn, tableName, row) {
      @Override
      public Boolean invokeCoprocessor(ThemisProtos.ThemisService.Stub instance) throws Throwable {
        ThemisProtos.LockExpiredRequest.Builder builder = ThemisProtos.LockExpiredRequest.newBuilder();
        builder.setTimestamp(timestamp);
        ServerRpcController controller = new ServerRpcController();
        BlockingRpcCallback<ThemisProtos.LockExpiredResponse> rpcCallback = new BlockingRpcCallback<>();
        instance.isLockExpired(controller, builder.build(),
                (com.google.protobuf.RpcCallback<ThemisProtos.LockExpiredResponse>) rpcCallback);
        checkRpcException(controller);
        return rpcCallback.get().getExpired();
    }}.run();
  }
}