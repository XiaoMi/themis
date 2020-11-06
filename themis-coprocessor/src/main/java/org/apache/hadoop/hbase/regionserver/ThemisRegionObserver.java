package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
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

public class ThemisRegionObserver implements RegionObserver, RegionCoprocessor {
  private static final Log LOG = LogFactory.getLog(ThemisRegionObserver.class);
  
  public static final String THEMIS_DELETE_THEMIS_DELETED_DATA_WHEN_COMPACT = "themis.delete.themis.deleted.data.when.compact";
  public static final String SINGLE_ROW_PRIMARY_QUALIFIER = "_themisSingleRowPrewritePrimaryQualifier_";
  public static final String LOCK_FAMILY_DELETE = "_themisLockFamilyDelete_";
  private boolean expiredDataCleanEnable;
  protected boolean deleteThemisDeletedDataWhenCompact;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    ColumnUtil.init(e.getConfiguration());
    expiredDataCleanEnable = e.getConfiguration().getBoolean(
      ThemisMasterObserver.THEMIS_EXPIRED_DATA_CLEAN_ENABLE_KEY, true);
    deleteThemisDeletedDataWhenCompact =  e.getConfiguration().getBoolean(
      THEMIS_DELETE_THEMIS_DELETED_DATA_WHEN_COMPACT, false);
    if (expiredDataCleanEnable) {
      LOG.info("themis expired data clean enable, deleteThemisDeletedDataWhenCompact=" + deleteThemisDeletedDataWhenCompact);
    }
  }

  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c, final Put put,
      final WALEdit edit, Durability durability) throws IOException {
    byte[] primaryQualifier = put.getAttribute(SINGLE_ROW_PRIMARY_QUALIFIER);
    if (primaryQualifier != null) {
      Region region = c.getEnvironment().getRegion();
      List<Cell> kvs = put.getFamilyCellMap().get(ColumnUtil.LOCK_FAMILY_NAME);
      if (kvs.size() != put.size() || kvs.size() == 0) {
        throw new IOException(
            "contain no-lock family kvs when do prewrite for single row transaction, put=" + put);
      }

      HStore lockStore = (HStore) region.getStore(ColumnUtil.LOCK_FAMILY_NAME);

      // we must make sure all the kvs of lock family be written to memstore at the same time,
      // if not, secondary lock kvs might be written firstly, snapshot and flushed while primary
      // kv not, which will break the atomic of transaction if region server is crashed before
      // primary kv flushed(although this seems won't cause problem single row transaction of themis)
      lockStore.lock.readLock().lock();
      try {
        // we must write lock for primary firstly
        int primaryIndex = -1;
        for (int i = 0; i < kvs.size(); ++i) {
          if (Bytes.equals(primaryQualifier, kvs.get(i).getQualifierArray())) {
            primaryIndex = i;
          }
        }

        if (primaryIndex < 0) {
          throw new IOException("can't find primary for single row transaction, primaryQualifier="
              + Bytes.toString(primaryQualifier) + ", put=" + put);
        }

        KeyValue newCell = new KeyValue(kvs.get(primaryIndex));
        // visible by any read
        newCell.setTimestamp(0);
        kvs.set(primaryIndex, newCell);

        MemStoreSizing memstoreAccounting = new NonThreadSafeMemStoreSizing();
        lockStore.memstore.add(kvs.get(primaryIndex), memstoreAccounting);

        // then, we write secondaries' locks
        for (int i = 0; i < kvs.size(); ++i) {
          if (i != primaryIndex) {
            newCell = new KeyValue(kvs.get(primaryIndex));
            newCell.setTimestamp(0);
            kvs.set(i, newCell);
            lockStore.memstore.add(kvs.get(i), memstoreAccounting);
          }
        }
      } finally {
        lockStore.lock.readLock().unlock();
      }
      // TODO : we don't do requestFlush judge here because lock family's write only take small part
      //        of memory. There is a corner case when there are only prewrites for single row transaction,
      //        we need to avoid memstore exceeds upper bound in this situation
      // TODO : keep region size consistent with memestore size(move to finally)
      //memstoreAccounting has been updated
      c.bypass();
    }
  }

  @Override
  public void preFlushScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, ScanOptions options, FlushLifeCycleTracker tracker) throws IOException {
    if (expiredDataCleanEnable && (ThemisMasterObserver.isThemisEnableFamily(store.getColumnFamilyDescriptor())
            || ColumnUtil.isCommitFamily(store.getColumnFamilyDescriptor().getName()))) {
      getScannerToCleanExpiredThemisData(store, ScanType.COMPACT_DROP_DELETES, options, true);
    }
  }

  @Override
  public void preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store, ScanType scanType,
                                    ScanOptions options, CompactionLifeCycleTracker tracker,
                                    CompactionRequest request) throws IOException {
    if (expiredDataCleanEnable
            && (ThemisMasterObserver.isThemisEnableFamily(store.getColumnFamilyDescriptor()) || ColumnUtil
            .isCommitFamily(store.getColumnFamilyDescriptor().getName()))) {
        getScannerToCleanExpiredThemisData(store, scanType, options, true);
    }
  }

  private void getScannerToCleanExpiredThemisData(Store store, ScanType scanType, ScanOptions options, boolean isCompact) {
    HStore hStore = (HStore) store;
    long cleanTs;

    ZKWatcher zk = hStore.getHRegion().getRegionServerServices().getZooKeeper();
    try {
      cleanTs = ThemisMasterObserver.getThemisExpiredTsFromZk(zk);
    } catch (Exception e) {
      LOG.error("themis region oberver get cleanTs fail, region="
          + hStore.getRegionInfo().getEncodedName() + ", family="
          + hStore.getColumnFamilyName() + ", scanType=" + scanType, e);
      return;
    }

    if (cleanTs == Long.MIN_VALUE) {
      LOG.warn("can't get a valid cleanTs, region=" + hStore.getRegionInfo().getEncodedName()
            + ", family=" + store.getColumnFamilyName() + ", scanType=" + scanType
            + ", please check zk path:" + ThemisMasterObserver.getThemisExpiredTsZNodePath(zk)
            + " is valid");
      return;
    }

    Scan scan = options.getScan();
    scan.setMaxVersions(hStore.getScanInfo().getMaxVersions());

    ThemisExpiredDataCleanFilter filter = null;
    if (deleteThemisDeletedDataWhenCompact && isCompact) {
      filter = new ThemisExpiredDataCleanFilter(cleanTs, hStore.getHRegion());
    } else {
      filter = new ThemisExpiredDataCleanFilter(cleanTs);
    }
    scan.setFilter(filter);
    LOG.info("themis clean data, add expired data clean filter for region="
        + hStore.getRegionInfo().getEncodedName() + ", family="
        + hStore.getColumnFamilyName() + ", ScanType=" + scanType
        + "cleanTs=" + cleanTs);
}

}
