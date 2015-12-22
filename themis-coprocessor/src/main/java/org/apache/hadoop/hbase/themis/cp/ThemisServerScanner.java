package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerStatus;

// themis scanner wrapper for RegionScanner which will be created by ThemisScanObserver
public class ThemisServerScanner implements RegionScanner {
  private final Scan scan;
  private final Scan dataScan;
  private final RegionScanner scanner;
  private final long startTs;
  private final int rawLimit;
  
  public Filter getDataColumnFilter() {
    return dataScan.getFilter();
  }

  public ThemisServerScanner(RegionScanner scanner, long startTs) {
    this(scanner, null, startTs, null);
  }
  
  public ThemisServerScanner(RegionScanner scanner, Scan scan, long startTs, Scan dataScan) {
    this.scan = scan;
    this.scanner = scanner;
    this.startTs = startTs;
    this.dataScan = dataScan;
    this.rawLimit = scan == null ? -1 : scan.getRawLimit();
  }
  
  public Scan getScan() {
    return this.scan;
  }
  
  public Scan getDataScan() {
    return this.dataScan;
  }
  
  public ScannerStatus next(List<KeyValue> results) throws IOException {
    return scanner.next(results);
  }

  public ScannerStatus next(List<KeyValue> results, String metric) throws IOException {
    return scanner.next(results, metric);
  }

  public ScannerStatus next(List<KeyValue> result, int limit, final int rawLimit) throws IOException {
    return scanner.next(result, limit, rawLimit);
  }

  public ScannerStatus next(List<KeyValue> result, int limit, final int rawLimit, String metric) throws IOException {
    return scanner.next(result, limit, rawLimit, metric);
  }
  
  public ScannerStatus nextRaw(List<KeyValue> result, final int rawLimit, String metric) throws IOException {
    return scanner.nextRaw(result, rawLimit, metric);
  }

  public ScannerStatus nextRaw(List<KeyValue> result, int limit, final int rawLimit, String metric) throws IOException {
    return scanner.nextRaw(result, limit, rawLimit, metric);
  }
  
  public long getMvccReadPoint() {
    return scanner.getMvccReadPoint();
  }
  
  public void close() throws IOException {
    scanner.close();
  }

  public HRegionInfo getRegionInfo() {
    return scanner.getRegionInfo();
  }

  public boolean isFilterDone() {
    return scanner.isFilterDone();
  }

  public boolean reseek(byte[] row) throws IOException {
    return scanner.reseek(row);
  }

  public long getStartTs() {
    return startTs;
  }

  public int getRawLimit() {
    return rawLimit;
  }

  public long getMaxResultSize() {
    return scanner.getMaxResultSize();
  }
}
