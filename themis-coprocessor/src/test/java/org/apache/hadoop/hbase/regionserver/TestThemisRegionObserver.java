package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
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
      ZooKeeperWatcher zk = new ZooKeeperWatcher(conf, "test", null, true);
      HBaseAdmin admin = new HBaseAdmin(connection);

      prewriteTestDataForPreFlushAndPreCompact();
      // no zk path
      ZKUtil.deleteNodeFailSilent(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk));
      admin.flush(TABLENAME);
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // invalid value of cleanTs
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(Long.MIN_VALUE)));
      admin.flush(TABLENAME);
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // cleanTs is too new
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(prewriteTs + 1)));
      admin.flush(TABLENAME);
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // clean old data
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(prewriteTs + 5)));
      admin.flush(TABLENAME);
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
  public void preCompactScannerOpen() throws Exception {
    // only test in MiniCluster
    if (TEST_UTIL != null) {
      ZooKeeperWatcher zk = new ZooKeeperWatcher(conf, "test", null, true);
      HBaseAdmin admin = new HBaseAdmin(connection);

      prewriteTestDataForPreFlushAndPreCompact();
      // no zk path
      ZKUtil.deleteNodeFailSilent(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk));
      admin.flush(TABLENAME);
      admin.compact(TABLENAME);
      Threads.sleep(5000); // wait compaction complete
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // invalid value of cleanTs
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(Long.MIN_VALUE)));
      admin.flush(TABLENAME);
      admin.compact(TABLENAME);
      Threads.sleep(5000); // wait compaction complete
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // cleanTs is too new
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(prewriteTs + 1)));
      admin.flush(TABLENAME);
      admin.compact(TABLENAME);
      Threads.sleep(5000); // wait compaction complete
      Assert.assertEquals(6, getRowByScan().size());
      deleteOldDataAndUpdateTs();

      // clean old data
      prewriteTestDataForPreFlushAndPreCompact();
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(Long.MIN_VALUE)));
      admin.flush(TABLENAME); // cleanTs is invalid when flush
      ZKUtil.createSetData(zk, ThemisMasterObserver.getThemisExpiredTsZNodePath(zk),
        Bytes.toBytes(String.valueOf(prewriteTs + 5)));
      // request majorCompact to make sure compact will be executed and cleanTs is valid when compact
      admin.majorCompact(TABLENAME);
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
}
