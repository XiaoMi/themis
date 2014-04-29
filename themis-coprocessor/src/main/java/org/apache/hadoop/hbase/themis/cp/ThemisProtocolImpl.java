package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import com.google.common.collect.Lists;

public class ThemisProtocolImpl extends BaseEndpointCoprocessor implements ThemisProtocol {
  private static final Log LOG = LogFactory.getLog(ThemisProtocolImpl.class);
  private static final byte[] EMPTY_BYTES = new byte[0];
  
  // TODO(cuijianwei) : read out data/lock/write column in the same region.get to improve efficiency?
  public Result themisGet(final Get get, final long startTs, final boolean ignoreLock)
      throws IOException {
    // first get lock and write columns to check conflicted lock and get commitTs
    Get lockAndWriteGet = ThemisCpUtil.constructLockAndWriteGet(get, startTs);
    HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    Result result = getFromRegion(region, lockAndWriteGet, null,
      ThemisCpStatistics.getThemisCpStatistics().getLockAndWriteLatency);
    Pair<List<KeyValue>, List<KeyValue>> lockAndWriteKvs = ThemisCpUtil.seperateLockAndWriteKvs(result.list());
    List<KeyValue> lockKvs = lockAndWriteKvs.getFirst();
    if (!ignoreLock && lockKvs.size() != 0) {
      LOG.warn("encounter conflict lock in ThemisGet"); // need to log conflict kv?
      // return lock columns when encounter conflict lock
      return new Result(lockKvs);
    }
    List<KeyValue> putKvs = ThemisCpUtil.getPutKvs(lockAndWriteKvs.getSecond());
    if (putKvs.size() == 0) {
      return new Result();
    }
    Get dataGet = ThemisCpUtil.constructDataGetByPutKvs(putKvs, get.getFilter());
    return getFromRegion(region, dataGet, null,
      ThemisCpStatistics.getThemisCpStatistics().getDataLatency);
  }
  
  protected Result getFromRegion(HRegion region, Get get, Integer lockId,
      MetricsTimeVaryingRate latency) throws IOException {
    long beginTs = System.nanoTime();
    try {
      return region.get(get, lockId);
    } finally {
      ThemisCpStatistics.updateLatency(latency, beginTs);
    }
  }
  
  abstract class MutationCallable<R> {
    private final byte[] row;
    public MutationCallable(byte[] row) {
      this.row = row;
    }
    
    public abstract R doMutation(HRegion region, Integer lid) throws IOException;
    
    public R run() throws IOException {
      HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
      Integer lid = region.getLock(null, row, true);
      // wait for all previous transactions to complete (with lock held)
      region.getMVCC().completeMemstoreInsert(region.getMVCC().beginMemstoreInsert());
      try {
        return doMutation(region, lid);
      } finally {
        region.releaseRowLock(lid);
      }
    }
  }
  
  protected void checkPrimaryLockAndIndex(final byte[] lockBytes, final int primaryIndex)
      throws IOException {
    // primaryIndex must be -1 when lockBytes is null, while primaryIndex must not be -1
    // when lockBytse is not null
    if ((lockBytes == null && primaryIndex != -1) || (lockBytes != null && primaryIndex == -1)) {
      throw new IOException("primaryLock is inconsistent with primaryIndex, primaryLock="
          + ThemisLock.parseFromByte(lockBytes) + ", primaryIndex=" + primaryIndex);
    }
  }
  
  public byte[][] prewriteRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final byte[] secondaryLock, final byte[] primaryLock,
      final int primaryIndex) throws IOException {
    checkPrimaryLockAndIndex(primaryLock, primaryIndex);
    return new MutationCallable<byte[][]>(row) {
      public byte[][] doMutation(HRegion region, Integer lid) throws IOException {
        // firstly, check conflict for each column
        // TODO : check one row one time to improve efficiency?
        for (ColumnMutation mutation : mutations) {
          byte[][] conflict = checkPrewriteConflict(region, row, lid, mutation, prewriteTs);
          if (conflict != null) {
            return conflict;
          }
        }

        Put prewritePut = new Put(row);
        for (int i = 0; i < mutations.size(); ++i) {
          ColumnMutation mutation = mutations.get(i);
          // get lock and set lock Type
          byte[] lockBytes = secondaryLock;
          if (primaryLock != null && i == primaryIndex) {
            lockBytes = primaryLock;
          }
          ThemisLock lock = ThemisLock.parseFromByte(lockBytes);
          lock.setType(mutation.getType());

          if (lock.getType().equals(Type.Put)) {
            prewritePut.add(mutation.getFamily(), mutation.getQualifier(), prewriteTs,
              mutation.getValue());
          }
          Column lockColumn = ColumnUtil.getLockColumn(mutation);
          prewritePut.add(lockColumn.getFamily(), lockColumn.getQualifier(), prewriteTs,
            ThemisLock.toByte(lock));
        }
        mutateToRegion(region, Lists.<Mutation>newArrayList(prewritePut),
          ThemisCpStatistics.getThemisCpStatistics().prewriteWriteLatency);
        return null;
      }
    }.run();
  }
  
  protected void mutateToRegion(HRegion region, List<Mutation> mutations,
      MetricsTimeVaryingRate latency) throws IOException {
    long beginTs = System.nanoTime();
    try {
      // we have obtained lock, do not need to require lock in mutateRowsWithLocks
      region.mutateRowsWithLocks(mutations, Collections.<byte[]>emptySet());
    } finally {
      ThemisCpStatistics.updateLatency(latency, beginTs);
    }
  }
  
  // check lock conflict and new write conflict. return null if no conflicts encountered; otherwise,
  // the first byte[] return the bytes of timestamp which is newer than prewriteTs, the secondary
  // byte[] will return the conflict lock if encounters lock conflict
  protected byte[][] checkPrewriteConflict(HRegion region, byte[] row, Integer lid,
      Column column, long prewriteTs) throws IOException {
    Column lockColumn = ColumnUtil.getLockColumn(column);
    // check no lock exist
    Get get = new Get(row).addColumn(lockColumn.getFamily(), lockColumn.getQualifier());
    Result result = getFromRegion(region, get, lid,
      ThemisCpStatistics.getThemisCpStatistics().prewriteReadLockLatency);
    byte[] existLockBytes = result.isEmpty() ? null : result.list().get(0).getValue();
    // check no newer write exist
    get = new Get(row);
    ThemisCpUtil.addWriteColumnToGet(column, get);
    get.setTimeRange(prewriteTs, Long.MAX_VALUE);
    result = getFromRegion(region, get, lid,
      ThemisCpStatistics.getThemisCpStatistics().prewriteReadWriteLatency);
    Long newerWriteTs = result.isEmpty() ? null : result.list().get(0).getTimestamp();
    byte[][] conflict = judgePrewriteConflict(column, existLockBytes, newerWriteTs);
    if (conflict != null) {
      LOG.warn("encounter conflict when prewrite, tableName="
          + Bytes.toString(region.getTableDesc().getName()) + ", row=" + Bytes.toString(row)
          + ", column=" + column + ", prewriteTs=" + prewriteTs);
    }
    return conflict;
  }
  
  protected byte[][] judgePrewriteConflict(Column column, byte[] existLockBytes, Long newerWriteTs) {
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
      return new byte[][]{Bytes.toBytes(newerWriteTs), existLockBytes, family, qualifier};
    }
    return null;
  }
  
  public boolean commitRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final long commitTs, final int primaryIndex) throws IOException {
    return new MutationCallable<Boolean>(row) {
      public Boolean doMutation(HRegion region, Integer lid) throws IOException {
        if (primaryIndex >= 0) {
          // can't commit the transaction if the primary lock has been erased
          ColumnMutation mutation = mutations.get(primaryIndex);
          byte[] lockBytes = readLockBytes(region, row, lid, mutation, prewriteTs,
            ThemisCpStatistics.getThemisCpStatistics().commitPrimaryReadLatency);
          if (lockBytes == null) {
            LOG.warn("primary lock erased, tableName="
                + Bytes.toString(region.getTableDesc().getName()) + ", row=" + Bytes.toString(row)
                + ", column=" + mutation + ", prewriteTs=" + prewriteTs);
            return false;
          }
        }
        doCommitMutations(region, row, lid, mutations, prewriteTs, commitTs);
        return true;
      }
    }.run();
  }

  // erase lock and put write column
  protected void doCommitMutations(HRegion region, byte[] row, Integer lid,
      List<ColumnMutation> mutations, long prewriteTs, long commitTs) throws IOException {
    List<Mutation> rowMutations = new ArrayList<Mutation>();
    for (ColumnMutation mutation : mutations) {
      Column writeColumn = mutation.getType() == Type.Put ? ColumnUtil.getPutColumn(mutation)
          : ColumnUtil.getDeleteColumn(mutation);
      Put writePut = new Put(row).add(writeColumn.getFamily(),
        writeColumn.getQualifier(), commitTs, Bytes.toBytes(prewriteTs));
      rowMutations.add(writePut);
      Column lockColumn = ColumnUtil.getLockColumn(mutation);
      Delete lockDelete = new Delete(row).deleteColumn(lockColumn.getFamily(),
        lockColumn.getQualifier(), prewriteTs);
      rowMutations.add(lockDelete);
    }
    mutateToRegion(region, rowMutations, ThemisCpStatistics.getThemisCpStatistics().commitWriteLatency);
  }
  
  protected byte[] readLockBytes(HRegion region, byte[] row, Integer lid, Column column,
      long prewriteTs, MetricsTimeVaryingRate latency) throws IOException {
    Column lockColumn = ColumnUtil.getLockColumn(column);
    Get get = new Get(row).addColumn(lockColumn.getFamily(), lockColumn.getQualifier());
    get.setTimeStamp(prewriteTs);
    Result result = getFromRegion(region, get, lid, latency);
    return result.isEmpty() ? null : result.list().get(0).getValue();
  }

  public byte[] getLockAndErase(final byte[] row, final byte[] family, final byte[] qualifier,
      final long prewriteTs) throws IOException {
    return new MutationCallable<byte[]>(row) {
      public byte[] doMutation(HRegion region, Integer lid) throws IOException {
        byte[] lockBytes = readLockBytes(region, row, lid, new Column(family, qualifier),
          prewriteTs, ThemisCpStatistics.getThemisCpStatistics().getLockAndEraseReadLatency);
        if (lockBytes == null) {
          return null;
        }
        
        Column lockColumn = ColumnUtil.getLockColumn(family, qualifier);
        List<Pair<Mutation, Integer>> mutationsAndLocks = new ArrayList<Pair<Mutation,Integer>>();
        Delete delete = new Delete(row);
        delete.deleteColumn(lockColumn.getFamily(), lockColumn.getQualifier(), prewriteTs);
        mutationsAndLocks.add(new Pair<Mutation, Integer>(delete, lid));
        region.batchMutate(mutationsAndLocks.toArray(new Pair[]{}));
        return lockBytes;
      }
    }.run();
  }
}
