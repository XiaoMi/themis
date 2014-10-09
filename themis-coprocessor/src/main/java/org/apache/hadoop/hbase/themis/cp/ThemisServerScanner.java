package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

// themis scanner wrapper for RegionScanner which will be created by ThemisScanObserver
public class ThemisServerScanner implements RegionScanner {
  private final Scan scan;
  private final Filter dataColumnFilter;
  private final RegionScanner scanner;
  private final long startTs;
  
  public Filter getDataColumnFilter() {
    return dataColumnFilter;
  }

  public ThemisServerScanner(RegionScanner scanner, long startTs) {
    this(scanner, null, startTs, null);
  }
  
  public ThemisServerScanner(RegionScanner scanner, Scan scan, long startTs, Filter dataColumnFilter) {
    this.scan = scan;
    this.scanner = scanner;
    this.startTs = startTs;
    this.dataColumnFilter = dataColumnFilter;
  }
  
  public Scan getScan() {
    return this.scan;
  }
  
  public boolean next(List<KeyValue> results) throws IOException {
    return scanner.next(results);
  }

  public boolean next(List<KeyValue> results, String metric) throws IOException {
    return scanner.next(results, metric);
  }

  public boolean next(List<KeyValue> result, int limit) throws IOException {
    return scanner.next(result, limit);
  }

  public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
    return scanner.next(result, limit, metric);
  }
  
  public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
    return scanner.nextRaw(result, metric);
  }

  public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
    return scanner.nextRaw(result, limit, metric);
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
}