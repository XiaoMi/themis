package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class ThemisRegionObserver extends BaseRegionObserver {
  public static final String SINGLE_ROW_PRIMARY_QUALIFIER = "_themisSingleRowPrewritePrimaryQualifier_";
  public static final String LOCK_FAMILY_DELETE = "_themisLockFamilyDelete_";

  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c, final Put put,
      final WALEdit edit, final boolean writeToWAL) throws IOException {
    byte[] primaryQualifier = put.getAttribute(SINGLE_ROW_PRIMARY_QUALIFIER);
    if (primaryQualifier != null) {
      HRegion region = c.getEnvironment().getRegion();
      List<KeyValue> kvs = put.getFamilyMap().get(ColumnUtil.LOCK_FAMILY_NAME);
      if (kvs.size() != put.size() || kvs.size() == 0) {
        throw new IOException(
            "contain no-lock family kvs when do prewrite for single row transaction, put=" + put);
      }

      Store lockStore = region.getStore(ColumnUtil.LOCK_FAMILY_NAME);
      long addedSize = 0;
      
      // we must make sure all the kvs of lock family be written to memstore at the same time,
      // if not, secondary lock kvs might be written firstly, snapshot and flushed while primary
      // kv not, which will break the atomic of transaction if region server is crashed before
      // primary kv flushed(although this seems won't cause problem single row transaction of themis)
      lockStore.lock.readLock().lock();
      try {
        // we must write lock for primary firstly
        int primaryIndex = -1;
        for (int i = 0; i < kvs.size(); ++i) {
          if (Bytes.equals(primaryQualifier, kvs.get(i).getQualifier())) {
            primaryIndex = i;
          }
        }

        if (primaryIndex < 0) {
          throw new IOException("can't find primary for single row transaction, primaryQualifier="
              + Bytes.toString(primaryQualifier) + ", put=" + put);
        }

        kvs.get(primaryIndex).setMemstoreTS(0); // visible by any read
        addedSize += lockStore.memstore.add(kvs.get(primaryIndex));

        // then, we write secondaries' locks
        for (int i = 0; i < kvs.size(); ++i) {
          if (i != primaryIndex) {
            kvs.get(i).setMemstoreTS(0);
            addedSize += lockStore.memstore.add(kvs.get(i));
          }
        }
      } finally {
        lockStore.lock.readLock().unlock();
      }
      // TODO : we don't do requestFlush judge here because lock family's write only take small part
      //        of memory. There is a corner case when there are only prewrites for single row transaction,
      //        we need to avoid memstore exceeds upper bound in this situation
      // TODO : keep region size consistent with memestore size(move to finally)
      region.addAndGetGlobalMemstoreSize(addedSize);
      c.bypass();
    }
  }
}
