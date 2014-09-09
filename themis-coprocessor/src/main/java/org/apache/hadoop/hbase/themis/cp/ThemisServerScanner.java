package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
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
  
  public boolean next(List<Cell> results) throws IOException {
    return scanner.next(results);
  }

  public boolean next(List<Cell> result, int limit) throws IOException {
    return scanner.next(result, limit);
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

  public boolean isFilterDone() throws IOException {
    return scanner.isFilterDone();
  }

  public boolean reseek(byte[] row) throws IOException {
    return scanner.reseek(row);
  }

  public long getMaxResultSize() {
    return scanner.getMaxResultSize();
  }

  public boolean nextRaw(List<Cell> result) throws IOException {
    return scanner.nextRaw(result);
  }

  public boolean nextRaw(List<Cell> result, int limit) throws IOException {
    return scanner.nextRaw(result, limit);
  }
}