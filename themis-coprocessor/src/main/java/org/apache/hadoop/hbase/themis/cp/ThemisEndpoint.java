package org.apache.hadoop.hbase.themis.cp;

import com.google.common.collect.Lists;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.ThemisRegionObserver;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.EraseLockRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.EraseLockResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.LockExpiredRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.LockExpiredResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisCommitRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisCommitResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisGetRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisPrewriteRequest;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisPrewriteResponse;
import org.apache.hadoop.hbase.themis.cp.generated.ThemisProtos.ThemisService;
import org.apache.hadoop.hbase.themis.exception.TransactionExpiredException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;

public class ThemisEndpoint extends ThemisService implements RegionCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(ThemisEndpoint.class);
  private static final byte[] EMPTY_BYTES = new byte[0];
  private RegionCoprocessorEnvironment env;

  @Override
  public void start(@SuppressWarnings("rawtypes") CoprocessorEnvironment env) throws IOException {
    // super.start(env);
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
      ColumnUtil.init(env.getConfiguration());
      TransactionTTL.init(env.getConfiguration());
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
  }

  @Override
  public void themisGet(RpcController controller, ThemisGetRequest request,
      RpcCallback<org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result> callback) {
    // first get lock and write columns to check conflicted lock and get commitTs
    ClientProtos.Result clientResult = ProtobufUtil.toResult(new Result());
    try {
      Region region = env.getRegion();
      Get clientGet = ProtobufUtil.toGet(request.getGet());
      ThemisCpUtil.prepareGet(clientGet, region.getTableDescriptor().getColumnFamilies());
      checkFamily(clientGet);
      checkReadTTL(System.currentTimeMillis(), request.getStartTs(), clientGet.getRow());
      Get lockAndWriteGet = ThemisCpUtil.constructLockAndWriteGet(clientGet, request.getStartTs());
      Result result =
        ThemisCpUtil.removeNotRequiredLockColumns(clientGet.getFamilyMap(), getFromRegion(region,
          lockAndWriteGet, ThemisCpStatistics.getThemisCpStatistics().getLockAndWriteLatency));
      Pair<List<Cell>, List<Cell>> lockAndWriteKvs =
        ThemisCpUtil.seperateLockAndWriteKvs(result.rawCells());
      List<Cell> lockKvs = lockAndWriteKvs.getFirst();
      if (!request.getIgnoreLock() && lockKvs.size() != 0) {
        LOG.warn("encounter conflict lock in ThemisGet"); // need to log conflict kv?
        // return lock columns when encounter conflict lock
        clientResult = ProtobufUtil.toResult(Result.create(lockKvs));
      } else {
        List<Cell> putKvs = ThemisCpUtil.getPutKvs(lockAndWriteKvs.getSecond());
        if (!putKvs.isEmpty()) {
          Get dataGet = ThemisCpUtil.constructDataGetByPutKvs(putKvs, clientGet.getFilter());
          clientResult = ProtobufUtil.toResult(getFromRegion(region, dataGet,
            ThemisCpStatistics.getThemisCpStatistics().getDataLatency));
        }
      }
    } catch (IOException e) {
      LOG.error("themisGet fail", e);
      // Call ServerRpcController#getFailedOn() to retrieve this IOException at client side.
      ResponseConverter.setControllerException(controller, e);
    }
    callback.run(clientResult);
  }

  public static void checkReadTTL(long currentMs, long startTs, byte[] row)
      throws TransactionExpiredException {
    if (!TransactionTTL.transactionTTLEnable) {
      return;
    }
    long expiredTimestamp = TransactionTTL.getExpiredTimestampForReadByCommitColumn(currentMs);
    if (startTs < TransactionTTL.getExpiredTimestampForReadByCommitColumn(currentMs)) {
      throw new TransactionExpiredException(
        "Expired Read Transaction, read transaction start Ts:" + startTs + ", expired Ts:" +
          expiredTimestamp + ", currentMs=" + currentMs + ", row=" + Bytes.toString(row));
    }
  }

  public static void checkWriteTTL(long currentMs, long startTs, byte[] row)
      throws TransactionExpiredException {
    if (!TransactionTTL.transactionTTLEnable) {
      return;
    }
    long expiredTimestamp = TransactionTTL.getExpiredTimestampForWrite(currentMs);
    if (startTs < expiredTimestamp) {
      throw new TransactionExpiredException(
        "Expired Write Transaction, write transaction start Ts:" + startTs + ", expired Ts:" +
          expiredTimestamp + ", currentMs=" + currentMs + ", row=" + Bytes.toString(row));
    }
  }

  private Result getFromRegion(Region region, Get get, MetricsTimeVaryingRate latency)
      throws IOException {
    long beginTs = System.nanoTime();
    try {
      return region.get(get);
    } finally {
      ThemisCpStatistics.updateLatency(latency, beginTs,
        "row=" + Bytes.toStringBinary(get.getRow()));
    }
  }

  @Override
  public void commitRow(RpcController controller, ThemisCommitRequest request,
      RpcCallback<ThemisCommitResponse> callback) {
    commitRow(controller, request, callback, false);
  }

  @Override
  public void commitSingleRow(RpcController controller, ThemisCommitRequest request,
      RpcCallback<ThemisCommitResponse> callback) {
    commitRow(controller, request, callback, true);
  }

  protected void commitRow(RpcController controller, ThemisCommitRequest request,
      RpcCallback<ThemisCommitResponse> callback, boolean isSingleRow) {
    boolean result = false;
    try {
      result = commitRow(request.getRow().toByteArray(),
        ColumnMutation.toColumnMutations(request.getMutationsList()), request.getPrewriteTs(),
        request.getCommitTs(), request.getPrimaryIndex(), isSingleRow);
    } catch (IOException e) {
      LOG.error("commitRow fail", e);
      ResponseConverter.setControllerException(controller, e);
    }
    ThemisCommitResponse.Builder builder = ThemisCommitResponse.newBuilder();
    builder.setResult(result);
    callback.run(builder.build());
  }

  @Override
  public void getLockAndErase(RpcController controller, EraseLockRequest request,
      RpcCallback<EraseLockResponse> callback) {
    byte[] lock = null;
    try {
      lock = getLockAndErase(request.getRow().toByteArray(), request.getFamily().toByteArray(),
        request.getQualifier().toByteArray(), request.getPrewriteTs());
    } catch (IOException e) {
      LOG.error("getLockAndErase fail", e);
      ResponseConverter.setControllerException(controller, e);
    }
    EraseLockResponse.Builder builder = EraseLockResponse.newBuilder();
    if (lock != null) {
      builder.setLock(HBaseZeroCopyByteString.wrap(lock));
    }
    callback.run(builder.build());
  }

  @Override
  public void prewriteRow(RpcController controller, ThemisPrewriteRequest request,
      RpcCallback<ThemisPrewriteResponse> callback) {
    prewriteRow(controller, request, callback, false);
  }

  @Override
  public void prewriteSingleRow(RpcController controller, ThemisPrewriteRequest request,
      RpcCallback<ThemisPrewriteResponse> callback) {
    prewriteRow(controller, request, callback, true);
  }

  protected void prewriteRow(RpcController controller, ThemisPrewriteRequest request,
      RpcCallback<ThemisPrewriteResponse> callback, boolean isSingleRow) {
    byte[][] prewriteResult = null;
    try {
      prewriteResult = prewriteRow(request.getRow().toByteArray(),
        ColumnMutation.toColumnMutations(request.getMutationsList()), request.getPrewriteTs(),
        request.getSecondaryLock().toByteArray(), request.getPrimaryLock().toByteArray(),
        request.getPrimaryIndex(), isSingleRow);
    } catch (IOException e) {
      LOG.error("prewrite fail", e);
      ResponseConverter.setControllerException(controller, e);
    }
    ThemisPrewriteResponse.Builder builder = ThemisPrewriteResponse.newBuilder();
    if (prewriteResult != null) {
      for (byte[] element : prewriteResult) {
        builder.addResult(HBaseZeroCopyByteString.wrap(element));
      }
    }
    callback.run(builder.build());
  }

  abstract class MutationCallable<R> {
    private final byte[] row;

    public MutationCallable(byte[] row) {
      this.row = row;
    }

    public abstract R doMutation(Region region, RowLock rowLock) throws IOException;

    public R run() throws IOException {
      Region region = env.getRegion();
      RowLock rowLock = region.getRowLock(row, true);
      // wait for all previous transactions to complete (with lock held)
      // region.getMVCC().completeMemstoreInsert(region.getMVCC().beginMemstoreInsert());
      try {
        return doMutation(region, rowLock);
      } finally {
        rowLock.release();
      }
    }
  }

  protected void checkPrimaryLockAndIndex(final byte[] lockBytes, final int primaryIndex)
      throws IOException {
    // primaryIndex must be -1 when lockBytes is null, while primaryIndex must not be -1
    // when lockBytse is not null
    if (((lockBytes == null || lockBytes.length == 0) && primaryIndex != -1) ||
      ((lockBytes != null && lockBytes.length != 0) && primaryIndex == -1)) {
      throw new IOException("primaryLock is inconsistent with primaryIndex, primaryLock=" +
        ThemisLock.parseFromByte(lockBytes) + ", primaryIndex=" + primaryIndex);
    }
  }

  protected void checkFamily(final Get get) throws IOException {
    checkFamily(get.getFamilyMap().keySet().toArray(new byte[][] {}));
  }

  protected void checkFamily(final List<ColumnMutation> mutations) throws IOException {
    byte[][] families = new byte[mutations.size()][];
    for (int i = 0; i < mutations.size(); ++i) {
      families[i] = mutations.get(i).getFamily();
    }
    checkFamily(families);
  }

  protected void checkFamily(final byte[][] families) throws IOException {
    checkFamily(env.getRegion(), families);
  }

  protected static void checkFamily(Region region, byte[][] families) throws IOException {
    for (byte[] family : families) {
      Store store = region.getStore(family);
      if (store == null) {
        throw new DoNotRetryIOException("family : '" + Bytes.toString(family) +
          "' not found in table : " + region.getTableDescriptor().getTableName());
      }
      String themisEnable = Bytes.toString(
        store.getColumnFamilyDescriptor().getValue(ThemisMasterObserver.THEMIS_ENABLE_KEY_BYTES));
      if ((themisEnable == null || !Boolean.parseBoolean(themisEnable)) &&
        !ColumnUtil.isAuxiliaryFamily(store.getColumnFamilyDescriptor().getName())) {
        throw new DoNotRetryIOException("can not access family : '" + Bytes.toString(family) +
          "' because " + ThemisMasterObserver.THEMIS_ENABLE_KEY + " is not set");
      }
    }
  }

  public byte[][] prewriteRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final byte[] secondaryLock, final byte[] primaryLock,
      final int primaryIndex, final boolean singleRow) throws IOException {
    // TODO : use ms enough?
    final long beginTs = System.nanoTime();
    try {
      checkFamily(mutations);
      checkWriteTTL(System.currentTimeMillis(), prewriteTs, row);
      checkPrimaryLockAndIndex(primaryLock, primaryIndex);
      return new MutationCallable<byte[][]>(row) {
        public byte[][] doMutation(Region region, RowLock rowLock) throws IOException {
          // firstly, check conflict for each column
          // TODO : check one row one time to improve efficiency?
          for (ColumnMutation mutation : mutations) {
            // TODO : make sure, won't encounter a lock with the same timestamp
            byte[][] conflict = checkPrewriteConflict(region, row, mutation, prewriteTs);
            if (conflict != null) {
              return conflict;
            }
          }
          ThemisCpStatistics.updateLatency(
            ThemisCpStatistics.getThemisCpStatistics().prewriteCheckConflictRowLatency, beginTs,
            false);

          Put prewritePut = new Put(row);
          byte[] primaryQualifier = null;
          for (int i = 0; i < mutations.size(); ++i) {
            boolean isPrimary = false;
            ColumnMutation mutation = mutations.get(i);
            // get lock and set lock Type
            byte[] lockBytes = secondaryLock;
            if (primaryLock != null && i == primaryIndex) {
              lockBytes = primaryLock;
              isPrimary = true;
            }
            ThemisLock lock = ThemisLock.parseFromByte(lockBytes);
            lock.setType(mutation.getType());

            if (!singleRow && lock.getType().equals(Type.Put)) {
              prewritePut.addColumn(mutation.getFamily(), mutation.getQualifier(), prewriteTs,
                mutation.getValue());
            }
            Column lockColumn = ColumnUtil.getLockColumn(mutation);
            prewritePut.addColumn(lockColumn.getFamily(), lockColumn.getQualifier(), prewriteTs,
              ThemisLock.toByte(lock));

            if (isPrimary) {
              primaryQualifier = lockColumn.getQualifier();
            }
          }
          if (singleRow) {
            prewritePut.setAttribute(ThemisRegionObserver.SINGLE_ROW_PRIMARY_QUALIFIER,
              primaryQualifier);
          }
          mutateToRegion(region, row, Lists.<Mutation> newArrayList(prewritePut),
            ThemisCpStatistics.getThemisCpStatistics().prewriteWriteLatency);
          return null;
        }
      }.run();
    } finally {
      ThemisCpStatistics.updateLatency(
        ThemisCpStatistics.getThemisCpStatistics().prewriteTotalLatency, beginTs, false);
    }
  }

  protected void mutateToRegion(Region region, byte[] row, List<Mutation> mutations,
      MetricsTimeVaryingRate latency) throws IOException {
    long beginTs = System.nanoTime();
    try {
      // we have obtained lock, do not need to require lock in mutateRowsWithLocks
      region.mutateRowsWithLocks(mutations, Collections.<byte[]> emptySet(), -1, -1);
    } finally {
      ThemisCpStatistics.updateLatency(latency, beginTs,
        "row=" + Bytes.toStringBinary(row) + ", mutationCount=" + mutations.size());
    }
  }

  // check lock conflict and new write conflict. return null if no conflicts encountered; otherwise,
  // the first byte[] return the bytes of timestamp which is newer than prewriteTs, the secondary
  // byte[] will return the conflict lock if encounters lock conflict
  protected byte[][] checkPrewriteConflict(Region region, byte[] row, Column column,
      long prewriteTs) throws IOException {
    Column lockColumn = ColumnUtil.getLockColumn(column);
    // check no lock exist
    Get get = new Get(row).addColumn(lockColumn.getFamily(), lockColumn.getQualifier());
    Result result = getFromRegion(region, get,
      ThemisCpStatistics.getThemisCpStatistics().prewriteReadLockLatency);
    byte[] existLockBytes = result.isEmpty() ? null : CellUtil.cloneValue(result.rawCells()[0]);
    boolean lockExpired =
      existLockBytes == null ? false : isLockExpired(result.rawCells()[0].getTimestamp());
    // check no newer write exist
    get = new Get(row);
    ThemisCpUtil.addWriteColumnToGet(column, get);
    get.setTimeRange(prewriteTs, Long.MAX_VALUE);
    result = getFromRegion(region, get,
      ThemisCpStatistics.getThemisCpStatistics().prewriteReadWriteLatency);
    Long newerWriteTs = result.isEmpty() ? null : result.rawCells()[0].getTimestamp();
    byte[][] conflict = judgePrewriteConflict(column, existLockBytes, newerWriteTs, lockExpired);
    if (conflict != null) {
      LOG.warn("encounter conflict when prewrite, tableName=" +
        region.getTableDescriptor().getTableName() + ", row=" + Bytes.toStringBinary(row) +
        ", column=" + column + ", prewriteTs=" + prewriteTs);
    }
    return conflict;
  }

  protected byte[][] judgePrewriteConflict(Column column, byte[] existLockBytes, Long newerWriteTs,
      boolean lockExpired) {
    if (newerWriteTs != null || existLockBytes != null) {
      newerWriteTs = newerWriteTs == null ? 0 : newerWriteTs;
      existLockBytes = existLockBytes == null ? new byte[0] : existLockBytes;
      byte[] family = EMPTY_BYTES;
      byte[] qualifier = EMPTY_BYTES;
      if (existLockBytes.length != 0) {
        // must return the data column other than lock column
        family = column.getFamily();
        qualifier = column.getQualifier();
      }
      // we also return the conflict family and qualifier, then the client could know which column
      // encounter conflict when prewriting by row
      return new byte[][] { Bytes.toBytes(newerWriteTs), existLockBytes, family, qualifier,
        Bytes.toBytes(lockExpired) };
    }
    return null;
  }

  public boolean commitRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final long commitTs, final int primaryIndex, final boolean singleRow)
      throws IOException {
    long beginTs = System.nanoTime();
    try {
      checkFamily(mutations);
      if (primaryIndex != -1) {
        checkWriteTTL(System.currentTimeMillis(), prewriteTs, row);
      }
      return new MutationCallable<Boolean>(row) {
        public Boolean doMutation(Region region, RowLock rowLock) throws IOException {
          if (primaryIndex >= 0) {
            // can't commit the transaction if the primary lock has been erased
            ColumnMutation mutation = mutations.get(primaryIndex);
            byte[] lockBytes = readLockBytes(region, row, mutation, prewriteTs,
              ThemisCpStatistics.getThemisCpStatistics().commitPrimaryReadLatency);
            if (lockBytes == null) {
              LOG.warn("primary lock erased, tableName=" +
                region.getTableDescriptor().getTableName() + ", row=" + Bytes.toStringBinary(row) +
                ", column=" + mutation + ", prewriteTs=" + prewriteTs);
              boolean hasCommitted = hasCommitted(region, row, mutation, commitTs,
                ThemisCpStatistics.getThemisCpStatistics().commitPrimaryReadLatency);
              if (hasCommitted) {
                LOG.warn("Primary lock has been erased by self, primaryRow has been committed");
              }
              return hasCommitted;
            }
            // TODO : for single-row, sanity check secondary lock must hold
          }
          doCommitMutations(region, row, mutations, prewriteTs, commitTs, singleRow);
          return true;
        }
      }.run();
    } finally {
      ThemisCpStatistics.updateLatency(
        ThemisCpStatistics.getThemisCpStatistics().commitTotalLatency, beginTs, false);
    }
  }

  protected void doCommitMutations(Region region, byte[] row, List<ColumnMutation> mutations,
      long prewriteTs, long commitTs, boolean singleRow) throws IOException {
    List<Mutation> rowMutations = new ArrayList<Mutation>();
    for (ColumnMutation mutation : mutations) {
      // auxiliary column should stay quiet, neither not to write timestamp or delete lock
      if (ColumnUtil.isAuxiliaryColumn(mutation)) {
        Put auxiliaryPut = new Put(row);
        auxiliaryPut.addColumn(mutation.getFamily(), mutation.getQualifier(), commitTs,
          mutation.getValue());
        rowMutations.add(auxiliaryPut);
        continue;
      }

      Put writePut = new Put(row);
      Column writeColumn = null;
      if (mutation.getType() == Type.Put) {
        writeColumn = ColumnUtil.getPutColumn(mutation);
        // we do not write data in prewrite-phase for single-row
        if (singleRow) {
          writePut.addColumn(mutation.getFamily(), mutation.getQualifier(), prewriteTs,
            mutation.getValue());
        }
      } else {
        writeColumn = ColumnUtil.getDeleteColumn(mutation);
      }
      writePut.addColumn(writeColumn.getFamily(), writeColumn.getQualifier(), commitTs,
        Bytes.toBytes(prewriteTs));
      rowMutations.add(writePut);

      Column lockColumn = ColumnUtil.getLockColumn(mutation);
      Delete lockDelete =
        new Delete(row).addColumn(lockColumn.getFamily(), lockColumn.getQualifier(), prewriteTs);
      setLockFamilyDelete(lockDelete);
      rowMutations.add(lockDelete);
    }
    mutateToRegion(region, row, rowMutations,
      ThemisCpStatistics.getThemisCpStatistics().commitWriteLatency);
  }

  protected byte[] readLockBytes(Region region, byte[] row, Column column, long prewriteTs,
      MetricsTimeVaryingRate latency) throws IOException {
    Column lockColumn = ColumnUtil.getLockColumn(column);
    Get get = new Get(row).addColumn(lockColumn.getFamily(), lockColumn.getQualifier());
    get.setTimestamp(prewriteTs);
    Result result = getFromRegion(region, get, latency);
    return result.isEmpty() ? null : CellUtil.cloneValue(result.rawCells()[0]);
  }

  protected boolean hasCommitted(Region region, byte[] row, Column column, long commitTs,
      MetricsTimeVaryingRate latency) throws IOException {
    Get get = new Get(row);
    ThemisCpUtil.addWriteColumnToGet(column, get);
    get.setTimestamp(commitTs);
    Result result = getFromRegion(region, get, latency);
    return !result.isEmpty();
  }

  public byte[] getLockAndErase(final byte[] row, final byte[] family, final byte[] qualifier,
      final long prewriteTs) throws IOException {
    return new MutationCallable<byte[]>(row) {
      public byte[] doMutation(Region region, RowLock rowLock) throws IOException {
        byte[] lockBytes = readLockBytes(region, row, new Column(family, qualifier), prewriteTs,
          ThemisCpStatistics.getThemisCpStatistics().getLockAndEraseReadLatency);
        if (lockBytes == null) {
          return null;
        }

        Column lockColumn = ColumnUtil.getLockColumn(family, qualifier);
        Delete delete = new Delete(row);
        setLockFamilyDelete(delete);
        delete.addColumn(lockColumn.getFamily(), lockColumn.getQualifier(), prewriteTs);
        mutateToRegion(region, row, Lists.<Mutation> newArrayList(delete),
          ThemisCpStatistics.getThemisCpStatistics().getLockAndEraseReadLatency);
        return lockBytes;
      }
    }.run();
  }

  public static void setLockFamilyDelete(final Delete delete) {
    delete.setAttribute(ThemisRegionObserver.LOCK_FAMILY_DELETE, Bytes.toBytes(true));
  }

  @Override
  public void isLockExpired(RpcController controller, LockExpiredRequest request,
      RpcCallback<LockExpiredResponse> callback) {
    boolean isExpired = false;
    try {
      isExpired = isLockExpired(request.getTimestamp());
    } catch (IOException e) {
      LOG.error("isLockExpired fail", e);
      ResponseConverter.setControllerException(controller, e);
    }
    LockExpiredResponse.Builder builder = LockExpiredResponse.newBuilder();
    builder.setExpired(isExpired);
    callback.run(builder.build());
  }

  public boolean isLockExpired(long lockTimestamp) throws IOException {
    long currentMs = System.currentTimeMillis();
    return lockTimestamp < TransactionTTL.getExpiredTimestampForWrite(currentMs);
  }
}
