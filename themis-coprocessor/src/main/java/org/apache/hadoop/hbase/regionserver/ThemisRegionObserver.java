package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThemisRegionObserver implements RegionObserver, RegionCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(ThemisRegionObserver.class);
  public static final String THEMIS_DELETE_THEMIS_DELETED_DATA_WHEN_COMPACT =
    "themis.delete.themis.deleted.data.when.compact";
  public static final String SINGLE_ROW_PRIMARY_QUALIFIER =
    "_themisSingleRowPrewritePrimaryQualifier_";
  public static final String LOCK_FAMILY_DELETE = "_themisLockFamilyDelete_";

  private boolean expiredDataCleanEnable;
  protected boolean deleteThemisDeletedDataWhenCompact;

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(@SuppressWarnings("rawtypes") CoprocessorEnvironment e) throws IOException {
    ColumnUtil.init(e.getConfiguration());
    expiredDataCleanEnable = e.getConfiguration()
      .getBoolean(ThemisMasterObserver.THEMIS_EXPIRED_DATA_CLEAN_ENABLE_KEY, true);
    deleteThemisDeletedDataWhenCompact =
      e.getConfiguration().getBoolean(THEMIS_DELETE_THEMIS_DELETED_DATA_WHEN_COMPACT, false);
    if (expiredDataCleanEnable) {
      LOG.info("themis expired data clean enable, deleteThemisDeletedDataWhenCompact=" +
        deleteThemisDeletedDataWhenCompact);
    }
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
      Durability durability) throws IOException {
    byte[] primaryQualifier = put.getAttribute(SINGLE_ROW_PRIMARY_QUALIFIER);
    if (primaryQualifier == null) {
      return;
    }
    Region region = c.getEnvironment().getRegion();
    List<Cell> kvs = put.getFamilyCellMap().get(ColumnUtil.LOCK_FAMILY_NAME);
    if (kvs.size() != put.size() || kvs.size() == 0) {
      throw new IOException(
        "contain no-lock family kvs when do prewrite for single row transaction, put=" + put);
    }

    HStore lockStore = (HStore) region.getStore(ColumnUtil.LOCK_FAMILY_NAME);
    NonThreadSafeMemStoreSizing memstoreAccounting = new NonThreadSafeMemStoreSizing();
    // we must make sure all the kvs of lock family be written to memstore at the same time,
    // if not, secondary lock kvs might be written firstly, snapshot and flushed while primary
    // kv not, which will break the atomic of transaction if region server is crashed before
    // primary kv flushed(although this seems won't cause problem single row transaction of
    // themis)
    lockStore.lock.readLock().lock();
    try {
      // we must write lock for primary firstly
      int primaryIndex = -1;
      for (int i = 0; i < kvs.size(); ++i) {
        if (Bytes.equals(primaryQualifier, CellUtil.cloneQualifier(kvs.get(i)))) {
          primaryIndex = i;
        }
      }

      if (primaryIndex < 0) {
        throw new IOException("can't find primary for single row transaction, primaryQualifier=" +
          Bytes.toString(primaryQualifier) + ", put=" + put);
      }
      ((ExtendedCell) kvs.get(primaryIndex)).setSequenceId(0); // visible by any read
      lockStore.memstore.add(kvs.get(primaryIndex), memstoreAccounting);

      // then, we write secondaries' locks
      for (int i = 0; i < kvs.size(); ++i) {
        if (i != primaryIndex) {
          ((ExtendedCell) kvs.get(i)).setSequenceId(0); // visible by any read
          lockStore.memstore.add(kvs.get(i), memstoreAccounting);
        }
      }
    } finally {
      // TODO : we don't do requestFlush judge here because lock family's write only take small part
      // of memory. There is a corner case when there are only prewrites for single row transaction,
      // we need to avoid memstore exceeds upper bound in this situation
      // TODO : keep region size consistent with memestore size(move to finally)
      ((HRegion) region).incMemStoreSize(memstoreAccounting.getMemStoreSize());
      lockStore.lock.readLock().unlock();
    }
    c.bypass();
  }

  private boolean shouldClean(Store store) {
    return expiredDataCleanEnable &&
      (ThemisMasterObserver.isThemisEnableFamily(store.getColumnFamilyDescriptor()) ||
        ColumnUtil.isCommitFamily(store.getColumnFamilyDescriptor().getName()));
  }

  @Override
  public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
    if (shouldClean(store)) {
      return getScannerToCleanExpiredThemisData(c.getEnvironment().getRegion(), store, scanner,
        false);
    } else {
      return scanner;
    }
  }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
      CompactionRequest request) throws IOException {
    if (shouldClean(store)) {
      return getScannerToCleanExpiredThemisData(c.getEnvironment().getRegion(), store, scanner,
        true);
    } else {
      return scanner;
    }
  }

  private InternalScanner getScannerToCleanExpiredThemisData(Region region, Store store,
      InternalScanner baseScanner, boolean isCompact) {
    long cleanTs = Long.MIN_VALUE;
    ZKWatcher zk = ((HStore) store).getHRegion().getRegionServerServices().getZooKeeper();
    try {
      cleanTs = ThemisMasterObserver.getThemisExpiredTsFromZk(zk);
    } catch (Exception e) {
      LOG.error("themis region oberver get cleanTs fail, region={}, family={}",
        store.getRegionInfo().getRegionNameAsString(), store.getColumnFamilyName(), e);
      return baseScanner;
    }
    if (cleanTs == Long.MIN_VALUE) {
      LOG.warn(
        "can't get a valid cleanTs, region={}, family={},  please check zk path: {} is valid",
        store.getRegionInfo().getRegionNameAsString(), store.getColumnFamilyName(),
        ThemisMasterObserver.getThemisExpiredTsZNodePath(zk));
      return baseScanner;
    }
    InternalScanner scanner;
    if (deleteThemisDeletedDataWhenCompact && isCompact) {
      scanner = new ThemisExpiredDataCleanScanner(cleanTs, baseScanner, region);
    } else {
      scanner = new ThemisExpiredDataCleanScanner(cleanTs, baseScanner);
    }
    LOG.info("themis clean data, add expired data clean filter for region=" +
      store.getRegionInfo().getRegionNameAsString() + ", family=" + store.getColumnFamilyName() +
      ", cleanTs=" + cleanTs + ", isCompact=" + isCompact);
    return scanner;
  }
}
