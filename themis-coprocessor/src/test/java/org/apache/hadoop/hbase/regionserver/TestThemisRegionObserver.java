package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL.TimestampType;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.Assert;
import org.junit.Test;

public class TestThemisRegionObserver extends TransactionTestBase {
  
  protected Result getRowByScan() throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions();
    ResultScanner scanner = getTable(TABLENAME).getScanner(scan);
    Result result = scanner.next();
    scanner.close();
    return result;
  }
  
  protected void prewriteTestDataForPreFlushAndPreCompact() throws IOException {
    writeData(COLUMN, prewriteTs);
    writeData(COLUMN, prewriteTs + 1);
    writePutColumn(COLUMN, prewriteTs, prewriteTs + 1);
    writePutColumn(COLUMN, prewriteTs + 1, prewriteTs + 2);
    writeDeleteColumn(COLUMN, prewriteTs + 2, prewriteTs + 3);
    writeDeleteColumn(COLUMN, prewriteTs + 3, prewriteTs + 4);
  }
  
  @Test
  public void testPreFlushScannerOpen() throws Exception {
    // only test in MiniCluster
    if (TEST_UTIL != null) {
      ZKWatcher zk = new ZKWatcher(conf, "test", null, true);
      Admin admin = connection.getAdmin();

      prewriteTestDataForPreFlushAndPreCompact();
      // no zk path
      ZKUtil.deleteNodeFailSilent(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk));
      admin.flush(TableName.valueOf(TABLENAME));
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // invalid value of cleanTs
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(Long.MIN_VALUE)));
      admin.flush(TableName.valueOf(TABLENAME));
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // cleanTs is too new
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(prewriteTs + 1)));
      admin.flush(TableName.valueOf(TABLENAME));
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // clean old data
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(prewriteTs + 5)));
      admin.flush(TableName.valueOf(TABLENAME));
      Result result = getRowByScan();
      Assert.assertEquals(3, result.size());
      Assert.assertNotNull(result.getValue(FAMILY, QUALIFIER));
      Column deleteColumn = ColumnUtil.getDeleteColumn(COLUMN);
      Assert.assertNotNull(result.getValue(deleteColumn.getFamily(), deleteColumn.getQualifier()));
      Column putColumn = ColumnUtil.getPutColumn(COLUMN);
      Assert.assertNotNull(result.getValue(putColumn.getFamily(), putColumn.getQualifier()));
      deleteOldDataAndUpdateTs();

      admin.close();
      zk.close();
    }
  }
  
  @Test
  public void testPreCompactScannerOpen() throws Exception {
    // only test in MiniCluster
    if (TEST_UTIL != null) {
      ZKWatcher zk = new ZKWatcher(conf, "test", null, true);
      Admin admin = connection.getAdmin();
      prewriteTestDataForPreFlushAndPreCompact();
      // no zk path
      ZKUtil.deleteNodeFailSilent(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk));
      admin.flush(TableName.valueOf(TABLENAME));
      admin.compact(TableName.valueOf(TABLENAME));
      Threads.sleep(5000); // wait compaction complete
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // invalid value of cleanTs
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(Long.MIN_VALUE)));
      admin.flush(TableName.valueOf(TABLENAME));
      admin.compact(TableName.valueOf(TABLENAME));
      Threads.sleep(5000); // wait compaction complete
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // cleanTs is too new
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(prewriteTs + 1)));
      admin.flush(TableName.valueOf(TABLENAME));
      admin.compact(TableName.valueOf(TABLENAME));
      Threads.sleep(5000); // wait compaction complete
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // clean old data
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(Long.MIN_VALUE)));
      admin.flush(TableName.valueOf(TABLENAME)); // cleanTs is invalid when flush
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(prewriteTs + 5)));
      admin.majorCompact(TableName.valueOf(TABLENAME)); // cleanTs is valid when compact
      Threads.sleep(5000); // wait compaction complete
      Result result = getRowByScan();
      Assert.assertEquals(3, result.size());
      Assert.assertNotNull(result.getValue(FAMILY, QUALIFIER));
      Column deleteColumn = ColumnUtil.getDeleteColumn(COLUMN);
      Assert.assertNotNull(result.getValue(deleteColumn.getFamily(), deleteColumn.getQualifier()));
      Column putColumn = ColumnUtil.getPutColumn(COLUMN);
      Assert.assertNotNull(result.getValue(putColumn.getFamily(), putColumn.getQualifier()));
      deleteOldDataAndUpdateTs();

      admin.close();
      zk.close();
    }
  }  

  @Test
  public void testPreCompactScannerOpenEnableDeletingThemisDeletedData() throws Exception {
    // only test in MiniCluster
    if (TEST_UTIL != null) {
      tearUp();
      TEST_UTIL.shutdownMiniCluster();
      // start the cluster and enable THEMIS_DELETE_THEMIS_DELETED_DATA_WHEN_COMPACT
      TransactionTestBase.useMiniCluster();
      conf.set(ThemisRegionObserver.THEMIS_DELETE_THEMIS_DELETED_DATA_WHEN_COMPACT, "true");
      TransactionTTL.timestampType = TimestampType.MS;
      TransactionTestBase.startMiniCluster(conf);
      initEnv();

      ZKWatcher zk = new ZKWatcher(conf, "test", null, true);
      Admin admin = connection.getAdmin();

      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(Long.MIN_VALUE)));
      admin.flush(TableName.valueOf(TABLENAME)); // cleanTs is invalid when flush
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(prewriteTs + 5)));
      admin.majorCompact(TableName.valueOf(TABLENAME)); // cleanTs is valid when compact
      Threads.sleep(5000); // wait compaction complete
      Result result = getRowByScan();
      Assert.assertNull(result);
      deleteOldDataAndUpdateTs();
      conf.set(ThemisRegionObserver.THEMIS_DELETE_THEMIS_DELETED_DATA_WHEN_COMPACT, "false");

      admin.close();
      zk.close();
    }
  }  
}
