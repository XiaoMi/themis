package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class ThemisScanObserver implements RegionObserver, Coprocessor {
  public static final String TRANSACTION_START_TS = "_themisTransationStartTs_";
  private static final byte[] PRE_SCANNER_OPEN_FEEK_ROW = Bytes.toBytes("preScannerOpen");
  private static final byte[] PRE_SCANNER_NEXT_FEEK_ROW = Bytes.toBytes("preScannerNext");
  private static final Log LOG = LogFactory.getLog(ThemisScanObserver.class);
  
  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    TransactionTTL.init(e.getConfiguration());
  }
  
  protected static byte[] currentRow(List<Cell> values) {
    return values.size() > 0 ? values.get(0).getRowArray() : PRE_SCANNER_NEXT_FEEK_ROW;
  }
  
  protected static boolean next(Region region, final ThemisServerScanner s,
      List<Result> results, int limit) throws IOException {
    List<Cell> values = new ArrayList<>();
    for (int i = 0; i < limit;) {
      try {
        boolean moreRows = s.next(values);
        ThemisProtocolImpl.checkReadTTL(System.currentTimeMillis(), s.getStartTs(),
          currentRow(values));
        if (!values.isEmpty()) {
          Result result = ThemisCpUtil.removeNotRequiredLockColumns(s.getDataScan().getFamilyMap(),
            Result.create(values));
          Pair<List<Cell>, List<Cell>> pResult = ThemisCpUtil
              .seperateLockAndWriteKvs(result.listCells());
          List<Cell> lockKvs = pResult.getFirst();
          if (lockKvs.size() == 0) {
            List<Cell> putKvs = ThemisCpUtil.getPutKvs(pResult.getSecond());
            // should ignore rows which only contain delete columns
            if (putKvs.size() > 0) {
              // TODO : check there must corresponding data columns by commit column
              Get dataGet = ThemisCpUtil.constructDataGetByPutKvs(putKvs, s.getDataColumnFilter());
              Result dataResult = region.get(dataGet);
              if (!dataResult.isEmpty()) {
                results.add(dataResult);
                ++i;
              }
            }
          } else {
            LOG.warn("encounter conflict lock in ThemisScan, row=" + result.getRow());
            results.add(Result.create(lockKvs));
            ++i;
          }
        }
        if (!moreRows) {
          return false;
        }
        values.clear();
      } catch (Throwable e) {
        LOG.error("themis error when scan.next for kvs=" + values);
        throw new IOException(e);
      }
    }
    return true;
  }
  
  // will do themis next logic if passing ThemisScanner
  @Override
  public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final List<Result> results,
      final int limit, final boolean hasMore) throws IOException {
    try {
      if (s instanceof ThemisServerScanner) {
        ThemisServerScanner pScanner = (ThemisServerScanner)s;
        Region region = e.getEnvironment().getRegion();
        boolean more = next(region, pScanner, results, limit);
        e.bypass();
        return more;
      }
      return hasMore;
    } catch (Throwable ex) {
      throw new DoNotRetryIOException("themis exception in preScannerNext", ex);
    }
  }

  // will create ThemisScanner when '_themisTransationStartTs_' is set in the attributes of scan;
  // otherwise, follow the origin read path to do hbase scan
  @Override
  public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e, final Scan scan, final RegionScanner regionScanner) throws IOException {
    try {
      Long themisStartTs = getStartTsFromAttribute(scan);
      if (themisStartTs != null) {
        ThemisCpUtil.prepareScan(scan, e.getEnvironment().getRegion().getTableDescriptor().getColumnFamilies());
        checkFamily(e.getEnvironment().getRegion(), scan);
        ThemisProtocolImpl.checkReadTTL(System.currentTimeMillis(), themisStartTs,
          PRE_SCANNER_OPEN_FEEK_ROW);
        Scan internalScan = ThemisCpUtil.constructLockAndWriteScan(scan, themisStartTs);
        ThemisServerScanner pScanner = new ThemisServerScanner(e.getEnvironment().getRegion()
            .getScanner(internalScan), internalScan, themisStartTs, scan);
        e.bypass();

        return pScanner;
      }

      return regionScanner;
    } catch (Throwable ex) {
      throw new DoNotRetryIOException("themis exception in preScannerOpen", ex);
    }
  }
  
  protected void checkFamily(final Region region, final Scan scan) throws IOException {
    ThemisProtocolImpl.checkFamily(region, scan.getFamilies());
  }
  
  public static Long getStartTsFromAttribute(Scan scan) {
    byte[] startTsBytes = scan.getAttribute(TRANSACTION_START_TS);
    return startTsBytes == null ? null : Bytes.toLong(startTsBytes);
  }
}