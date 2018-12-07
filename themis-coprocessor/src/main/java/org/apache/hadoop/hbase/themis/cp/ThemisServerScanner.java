package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

// themis scanner wrapper for RegionScanner which will be created by ThemisScanObserver
public class ThemisServerScanner implements RegionScanner {
  private final Scan userScan;
  private final RegionScanner scanner;
  private final long startTs;

  public Filter getDataColumnFilter() {
    return userScan.getFilter();
  }

  public ThemisServerScanner(RegionScanner scanner, long startTs, Scan userScan) {
    this.scanner = scanner;
    this.startTs = startTs;
    this.userScan = userScan;
  }

  public Scan getUserScan() {
    return this.userScan;
  }

  @Override
  public long getMvccReadPoint() {
    return scanner.getMvccReadPoint();
  }

  @Override
  public void close() throws IOException {
    scanner.close();
  }

  @Override
  public RegionInfo getRegionInfo() {
    return scanner.getRegionInfo();
  }

  @Override
  public boolean isFilterDone() throws IOException {
    return scanner.isFilterDone();
  }

  @Override
  public boolean reseek(byte[] row) throws IOException {
    return scanner.reseek(row);
  }

  @Override
  public long getMaxResultSize() {
    return scanner.getMaxResultSize();
  }

  public long getStartTs() {
    return startTs;
  }

  @Override
  public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
    return scanner.next(result, scannerContext);
  }

  @Override
  public int getBatch() {
    return scanner.getBatch();
  }

  @Override
  public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
    return scanner.nextRaw(result, scannerContext);
  }

  public boolean next(List<Cell> results) throws IOException {
    return scanner.next(results);
  }

  public boolean nextRaw(List<Cell> result) throws IOException {
    return scanner.next(result);
  }
}
