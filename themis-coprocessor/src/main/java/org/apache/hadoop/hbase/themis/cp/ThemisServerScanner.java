package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

// themis scanner wrapper for RegionScanner which will be created by ThemisScanObserver
public class ThemisServerScanner implements RegionScanner {
  private final Scan scan;
  private final Scan dataScan;
  private final RegionScanner scanner;
  private final long startTs;
  
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
  }

  public Scan getScan() {
    return this.scan;
  }
  
  public Scan getDataScan() {
    return this.dataScan;
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

  public long getStartTs() {
    return startTs;
  }

  public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
    return scanner.next(result, scannerContext);
  }

  public int getBatch() {
    return scanner.getBatch();
  }

  public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
    return scanner.nextRaw(result, scannerContext);
  }

  @Override
  public void close(boolean b) throws IOException {
    scanner.close(b);
  }

  public boolean next(List<Cell> results) throws IOException {
    return scanner.next(results);
  }

  public boolean nextRaw(List<Cell> result) throws IOException {
    return scanner.next(result);
  }
}
