package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.themis.util.ObjectUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

public class ThemisScanObserver implements RegionObserver, RegionCoprocessor {

  private static final Logger LOG = LoggerFactory.getLogger(ThemisScanObserver.class);

  public static final String TRANSACTION_START_TS = "_themisTransationStartTs_";
  private static final byte[] PRE_SCANNER_OPEN_FEEK_ROW = Bytes.toBytes("preScannerOpen");
  private static final byte[] PRE_SCANNER_NEXT_FEEK_ROW = Bytes.toBytes("preScannerNext");

  private static final Unsafe UNSAFE;

  static {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      UNSAFE = (Unsafe) f.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(@SuppressWarnings("rawtypes") CoprocessorEnvironment e) throws IOException {
    TransactionTTL.init(e.getConfiguration());
  }

  private static byte[] currentRow(List<Cell> values) {
    return values.size() > 0 ? CellUtil.cloneFamily(values.get(0)) : PRE_SCANNER_NEXT_FEEK_ROW;
  }

  private static boolean next(Region region, final ThemisServerScanner s, List<Result> results,
      int limit) throws IOException {
    List<Cell> values = new ArrayList<Cell>();
    for (int i = 0; i < limit;) {
      try {
        // TODO : use next or nextRaw ?
        boolean moreRows = s.next(values);
        ThemisEndpoint.checkReadTTL(System.currentTimeMillis(), s.getStartTs(), currentRow(values));
        if (!values.isEmpty()) {
          Result result = ThemisCpUtil.removeNotRequiredLockColumns(s.getUserScan().getFamilyMap(),
            Result.create(values));
          Pair<List<Cell>, List<Cell>> pResult =
            ThemisCpUtil.seperateLockAndWriteKvs(result.rawCells());
          List<Cell> lockKvs = pResult.getFirst();
          if (lockKvs.size() == 0) {
            List<Cell> putKvs = ThemisCpUtil.getPutKvs(pResult.getSecond());
            // should ignore rows which only contain delete columns
            if (putKvs.size() > 0) {
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
  public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s,
      List<Result> results, int limit, boolean hasMore) throws IOException {
    try {
      if (s instanceof ThemisServerScanner) {
        ThemisServerScanner pScanner = (ThemisServerScanner) s;
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

  private final ThreadLocal<Scan> userScanHolder = new ThreadLocal<>();

  // will create ThemisScanner when '_themisTransationStartTs_' is set in the attributes of scan;
  // otherwise, follow the origin read path to do hbase scan
  @Override
  public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
      throws IOException {
    Long themisStartTs = getStartTsFromAttribute(scan);
    if (themisStartTs == null) {
      return;
    }
    ThemisCpUtil.prepareScan(scan,
      c.getEnvironment().getRegion().getTableDescriptor().getColumnFamilies());
    ThemisEndpoint.checkReadTTL(System.currentTimeMillis(), themisStartTs,
      PRE_SCANNER_OPEN_FEEK_ROW);
    // here we return the internalScan to let the upper layer open a RegionScanner with the
    // internalScan. The original scan instance is stored using thread local, and in
    // postScannerOpen, we will fetch it and create the ThemisServerScanner.
    Scan internalScan = ThemisCpUtil.constructLockAndWriteScan(scan, themisStartTs);
    userScanHolder.set(new Scan(scan));
    copy(scan, internalScan);
  }

  private void copy(Scan to, Scan from) throws IOException {
    List<Field> allFileds = Lists.newArrayList();
    ObjectUtils.getAllField(Scan.class, allFileds);
    List<Field> fieldToCopy = allFileds.stream()
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        .filter(f -> {
          final String name = f.getName();
          return !("familyMap".equals(name) || "attributes".equals(name) || "colFamTimeRangeMap".equals(name));
        }).collect(Collectors.toList());

    ObjectUtils.copyScan(to, from, fieldToCopy);

    //handle fams
    Map<byte[], NavigableSet<byte[]>> toFamilyMap = to.getFamilyMap();
    Map<byte[], NavigableSet<byte[]>> fromFamilyMap = from.getFamilyMap();

    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : fromFamilyMap.entrySet()) {
      NavigableSet<byte[]> familys = toFamilyMap.get(entry.getKey());
      if (Objects.isNull(familys)) {
        toFamilyMap.put(entry.getKey(), entry.getValue());
      } else {
        familys.addAll(entry.getValue());
      }
    }

    try {
      ObjectUtils.setObject("familyMap", to, toFamilyMap);

      //handle
      Map<String, byte[]> copyAttributes = Maps.newHashMap(to.getAttributesMap());
      copyAttributes.putAll(from.getAttributesMap());
      ObjectUtils.setObject("attributes", to, copyAttributes);

      //handle
      Map<byte[], TimeRange> copyColumnFamilyTimeRange = Maps.newHashMap(to.getColumnFamilyTimeRange());
      copyColumnFamilyTimeRange.putAll(from.getColumnFamilyTimeRange());
      ObjectUtils.setObject("colFamTimeRangeMap", to, copyColumnFamilyTimeRange);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
      RegionScanner s) throws IOException {
    Long themisStartTs = getStartTsFromAttribute(scan);
    if (themisStartTs == null) {
      return s;
    }
    Scan userScan = userScanHolder.get();
    assert userScan != null;
    userScanHolder.remove();
    return new ThemisServerScanner(s, themisStartTs, userScan);
  }

  protected void checkFamily(Region region, Scan scan) throws IOException {
    ThemisEndpoint.checkFamily(region, scan.getFamilies());
  }

  public static Long getStartTsFromAttribute(Scan scan) {
    byte[] startTsBytes = scan.getAttribute(TRANSACTION_START_TS);
    return startTsBytes == null ? null : Bytes.toLong(startTsBytes);
  }
}
