package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

// themis scanner wrapper for RegionScanner which will be created by ThemisScanObserver
public class ThemisServerScanner implements RegionScanner {
  private Filter dataColumnFilter;
  private RegionScanner scanner;
  
  public Filter getDataColumnFilter() {
    return dataColumnFilter;
  }

  public ThemisServerScanner(RegionScanner scanner) {
    this(scanner, null);
  }
  
  public ThemisServerScanner(RegionScanner scanner, Filter dataColumnFilter) {
    this.scanner = scanner;
    this.dataColumnFilter = dataColumnFilter;
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
}