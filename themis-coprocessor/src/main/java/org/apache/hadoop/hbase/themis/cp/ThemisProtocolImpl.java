package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.ThemisRegionObserver;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.exception.TransactionExpiredException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import com.google.common.collect.Lists;

public class ThemisProtocolImpl extends BaseEndpointCoprocessor implements ThemisProtocol {
  private static final Log LOG = LogFactory.getLog(ThemisProtocolImpl.class);
  private static final byte[] EMPTY_BYTES = new byte[0];
  
  @Override
  public void start(CoprocessorEnvironment env) {
    super.start(env);
    TransactionTTL.init(env.getConfiguration());
  }
  
  // TODO(cuijianwei) : read out data/lock/write column in the same region.get to improve efficiency?
  public Result themisGet(final Get get, final long startTs, final boolean ignoreLock)
      throws IOException {
    checkFamily(get);
    checkReadTTL(System.currentTimeMillis(), startTs);
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
    // TODO : check there must corresponding data columns by commit column
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
      // region.getMVCC().completeMemstoreInsert(region.getMVCC().beginMemstoreInsert());
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
  
  public static void checkReadTTL(long currentMs, long startTs)
      throws TransactionExpiredException {
    long expiredTimestamp = TransactionTTL.getExpiredTimestampForReadByCommitColumn(currentMs);
    if (startTs < TransactionTTL.getExpiredTimestampForReadByCommitColumn(currentMs)) {
      throw new TransactionExpiredException("Expired Read Transaction, read transaction start Ts:"
          + startTs + ", expired Ts:" + expiredTimestamp + ", currentMs=" + currentMs);
    }
  }
  
  public static void checkWriteTTL(long currentMs, long startTs)
      throws TransactionExpiredException {
    long expiredTimestamp = TransactionTTL.getExpiredTimestampForWrite(currentMs);
    if (startTs < expiredTimestamp) {
      throw new TransactionExpiredException(
          "Expired Write Transaction, write transaction start Ts:" + startTs + ", expired Ts:"
              + expiredTimestamp + ", currentMs=" + currentMs);
    }
  }
  
  public byte[][] prewriteRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final byte[] secondaryLock, final byte[] primaryLock,
      final int primaryIndex) throws IOException {
    return prewriteRow(row, mutations, prewriteTs, secondaryLock, primaryLock, primaryIndex, false);
  }

  protected void checkFamily(final Get get) throws IOException {
    checkFamily(get.getFamilyMap().keySet().toArray(new byte[][]{}));
  }
  
  protected void checkFamily(final List<ColumnMutation> mutations) throws IOException {
    byte[][] families = new byte[mutations.size()][];
    for (int i = 0; i < mutations.size(); ++i) {
      families[i] = mutations.get(i).getFamily();
    }
    checkFamily(families);
  }
  
  protected void checkFamily(final byte[][] families) throws IOException {
    checkFamily(((RegionCoprocessorEnvironment) getEnvironment()).getRegion(), families);
  }
  
  protected static void checkFamily(HRegion region, byte[][] families) throws IOException {
    for (byte[] family : families) {
      Store store = region.getStore(family);
      if (store == null) {
        throw new DoNotRetryIOException("family : '" + family + "' not found in table : "
            + region.getTableDesc().getNameAsString());
      }
      String themisEnable = store.getFamily().getValue(ThemisMasterObserver.THEMIS_ENABLE_KEY);
      if (themisEnable == null || !Boolean.parseBoolean(themisEnable)) {
        throw new DoNotRetryIOException("can not access family : '" + Bytes.toString(family)
            + "' because " + ThemisMasterObserver.THEMIS_ENABLE_KEY + " is not set");
      }
    }
  }
  
  public byte[][] prewriteRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final byte[] secondaryLock, final byte[] primaryLock,
      final int primaryIndex, final boolean singleRow) throws IOException {
    checkFamily(mutations);
    // TODO : use ms enough?
    long beginTs = System.nanoTime();
    checkWriteTTL(System.currentTimeMillis(), prewriteTs);
    try {
      checkPrimaryLockAndIndex(primaryLock, primaryIndex);
      return new MutationCallable<byte[][]>(row) {
        public byte[][] doMutation(HRegion region, Integer lid) throws IOException {
          // firstly, check conflict for each column
          // TODO : check one row one time to improve efficiency?
          for (ColumnMutation mutation : mutations) {
            // TODO : make sure, won't encounter a lock with the same timestamp
            byte[][] conflict = checkPrewriteConflict(region, row, lid, mutation, prewriteTs);
            if (conflict != null) {
              return conflict;
            }
          }

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
              prewritePut.add(mutation.getFamily(), mutation.getQualifier(), prewriteTs,
                mutation.getValue());
            }
            Column lockColumn = ColumnUtil.getLockColumn(mutation);
            prewritePut.add(lockColumn.getFamily(), lockColumn.getQualifier(), prewriteTs,
              ThemisLock.toByte(lock));
            
            if (isPrimary) {
              primaryQualifier = lockColumn.getQualifier();
            }
          }
          if (singleRow) {
            prewritePut.setAttribute(ThemisRegionObserver.SINGLE_ROW_PRIMARY_QUALIFIER,
              primaryQualifier);
          }
          mutateToRegion(region, Lists.<Mutation> newArrayList(prewritePut),
            ThemisCpStatistics.getThemisCpStatistics().prewriteWriteLatency);
          return null;
        }
      }.run();
    } finally {
      ThemisCpStatistics.updateLatency(
        ThemisCpStatistics.getThemisCpStatistics().prewriteTotalLatency, beginTs);
    }
  }
  
  public byte[][] prewriteSingleRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final byte[] secondaryLock, final byte[] primaryLock,
      final int primaryIndex) throws IOException {
    return prewriteRow(row, mutations, prewriteTs, secondaryLock, primaryLock, primaryIndex, true);
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
    boolean lockExpired = existLockBytes == null ? false : isLockExpired(result.list().get(0)
        .getTimestamp());
    // check no newer write exist
    get = new Get(row);
    ThemisCpUtil.addWriteColumnToGet(column, get);
    get.setTimeRange(prewriteTs, Long.MAX_VALUE);
    result = getFromRegion(region, get, lid,
      ThemisCpStatistics.getThemisCpStatistics().prewriteReadWriteLatency);
    Long newerWriteTs = result.isEmpty() ? null : result.list().get(0).getTimestamp();
    byte[][] conflict = judgePrewriteConflict(column, existLockBytes, newerWriteTs, lockExpired);
    if (conflict != null) {
      LOG.warn("encounter conflict when prewrite, tableName="
          + Bytes.toString(region.getTableDesc().getName()) + ", row=" + Bytes.toString(row)
          + ", column=" + column + ", prewriteTs=" + prewriteTs);
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
      final long prewriteTs, final long commitTs, final int primaryIndex) throws IOException {
    return commitRow(row, mutations, prewriteTs, commitTs, primaryIndex, false);
  }
  
  public boolean commitRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final long commitTs, final int primaryIndex, final boolean singleRow)
      throws IOException {
    long beginTs = System.nanoTime();
    checkWriteTTL(System.currentTimeMillis(), prewriteTs);
    try {
      return new MutationCallable<Boolean>(row) {
        public Boolean doMutation(HRegion region, Integer lid) throws IOException {
          if (primaryIndex >= 0) {
            // can't commit the transaction if the primary lock has been erased
            ColumnMutation mutation = mutations.get(primaryIndex);
            byte[] lockBytes = readLockBytes(region, row, lid, mutation, prewriteTs,
              ThemisCpStatistics.getThemisCpStatistics().commitPrimaryReadLatency);
            if (lockBytes == null) {
              LOG.warn("primary lock erased, tableName="
                  + Bytes.toString(region.getTableDesc().getName()) + ", row="
                  + Bytes.toString(row) + ", column=" + mutation + ", prewriteTs=" + prewriteTs);
              return false;
            }
            // TODO : for single-row, sanity check secondary lock must hold
          }
          doCommitMutations(region, row, lid, mutations, prewriteTs, commitTs, singleRow);
          return true;
        }
      }.run();
    } finally {
      ThemisCpStatistics.updateLatency(
        ThemisCpStatistics.getThemisCpStatistics().commitTotalLatency, beginTs);
    }
  }
  
  public boolean commitSingleRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final long commitTs, final int primaryIndex) throws IOException {
    return commitRow(row, mutations, prewriteTs, commitTs, primaryIndex, true);
  }

  // erase lock and put write column
  protected void doCommitMutations(HRegion region, byte[] row, Integer lid,
      List<ColumnMutation> mutations, long prewriteTs, long commitTs) throws IOException {
    doCommitMutations(region, row, lid, mutations, prewriteTs, commitTs, false);
  }
  
  protected void doCommitMutations(HRegion region, byte[] row, Integer lid,
      List<ColumnMutation> mutations, long prewriteTs, long commitTs, boolean singleRow)
      throws IOException {
    List<Mutation> rowMutations = new ArrayList<Mutation>();
    for (ColumnMutation mutation : mutations) {
      Put writePut = new Put(row);
      Column writeColumn = null;
      if (mutation.getType() == Type.Put) {
        writeColumn = ColumnUtil.getPutColumn(mutation);
        // we do not write data in prewrite-phase for single-row
        if (singleRow) {
          writePut.add(mutation.getFamily(), mutation.getQualifier(), prewriteTs,
            mutation.getValue());
        }
      } else {
        writeColumn = ColumnUtil.getDeleteColumn(mutation);
      }
      writePut.add(writeColumn.getFamily(), writeColumn.getQualifier(), commitTs,
        Bytes.toBytes(prewriteTs));
      rowMutations.add(writePut);
      
      Column lockColumn = ColumnUtil.getLockColumn(mutation);
      Delete lockDelete = new Delete(row).deleteColumn(lockColumn.getFamily(),
        lockColumn.getQualifier(), prewriteTs);
      setLockFamilyDelete(lockDelete);
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
        Delete delete = new Delete(row);
        setLockFamilyDelete(delete);
        delete.deleteColumn(lockColumn.getFamily(), lockColumn.getQualifier(), prewriteTs);
        mutateToRegion(region, Lists.<Mutation> newArrayList(delete),
        ThemisCpStatistics.getThemisCpStatistics().getLockAndEraseReadLatency);
        return lockBytes;
      }
    }.run();
  }
  
  public static void setLockFamilyDelete(final Delete delete) {
    delete.setAttribute(ThemisRegionObserver.LOCK_FAMILY_DELETE, Bytes.toBytes(true));
  }

  public boolean isLockExpired(long lockTimestamp) throws IOException {
    long currentMs = System.currentTimeMillis();
    System.out.println("###debug, lockTimestamp=" + lockTimestamp + ", currentMs=" + currentMs
        + ", expiredTs=" + TransactionTTL.getExpiredTimestampForWrite(currentMs));
    return lockTimestamp < TransactionTTL.getExpiredTimestampForWrite(currentMs);
  }
}
