package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableSet;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.themis.cp.ThemisCpUtil;
import org.apache.hadoop.hbase.themis.cp.ThemisScanObserver;
import org.apache.hadoop.hbase.util.Bytes;

import com.xiaomi.infra.thirdparty.com.google.common.io.Closeables;

// scanner for range read
public class ThemisScanner extends AbstractClientScanner {
  protected final Table table;
  protected final ResultScanner scanner;
  protected final TableName tableName;
  protected Transaction transaction;
  protected final Scan scan;

  public ThemisScanner(TableName tableName, final Scan scan, final Transaction transaction)
      throws IOException {
    long beginTs = System.nanoTime();
    try {
      this.tableName = tableName;
      this.transaction = transaction;
      this.scan = scan;
      // we need to set startTs to the attribute named '_themisTransationStartTs_'. Then, the loaded
      // themis coprocessor could recognize this scanner from hbase scanners and do themis logics.
      // TODO(cuijianwei): how to avoid no-themis users set this attribute when doing hbase scan?
      setStartTsToScan(scan, transaction.startTs);
      this.table = transaction.getHConnection().getTable(tableName);
      this.scanner = table.getScanner(scan);
    } finally {
      ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().getScannerLatency, beginTs);
    }
  }

  protected static void setStartTsToScan(Scan scan, long startTs) {
    scan.setAttribute(ThemisScanObserver.TRANSACTION_START_TS, Bytes.toBytes(startTs));
  }

  public static Get createGetFromScan(Scan scan, byte[] rowkey) {
    Get get = new Get(rowkey);
    for (Entry<byte[], NavigableSet<byte[]>> familyEntry : scan.getFamilyMap().entrySet()) {
      if (familyEntry.getValue() != null && familyEntry.getValue().size() > 0) {
        for (byte[] qualifier : familyEntry.getValue()) {
          get.addColumn(familyEntry.getKey(), qualifier);
        }
      } else {
        get.addFamily(familyEntry.getKey());
      }
    }
    return get;
  }

  public Result next() throws IOException {
    long beginTs = System.nanoTime();
    Result pResult = null;
    boolean lockClean = false;
    try {
      pResult = this.scanner.next();
      if (pResult == null) {
        return null;
      }

      // if we encounter conflict locks, we need to clean lock for this row and read again
      if (ThemisCpUtil.isLockResult(pResult)) {
        lockClean = true;
        Get rowGet = createGetFromScan(scan, pResult.getRow());
        pResult = transaction.tryToCleanLockAndGetAgain(tableName, rowGet, pResult.listCells());
        // empty result indicates the current row has been erased, we should get next row
        if (pResult.isEmpty()) {
          return next();
        } else {
          return pResult;
        }
      }
      return pResult;
    } finally {
      ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().nextLatency, beginTs);
      ThemisStatistics.logSlowOperation("themisNext", beginTs,
        "row=" + pResult + ", lockClean=" + lockClean);
    }
  }

  public void close() {
    if (scanner != null) {
      this.scanner.close();
    }
    try {
      Closeables.close(table, true);
    } catch (IOException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  protected Scan getScan() {
    return this.scan;
  }

  @Override
  public boolean renewLease() {
    return scanner.renewLease();
  }
}