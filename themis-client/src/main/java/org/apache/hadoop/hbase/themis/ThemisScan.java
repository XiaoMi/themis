package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

//a wrapper class of Scan in HBase which not expose timestamp to user
public class ThemisScan extends ThemisRead {
  private Scan scan;
  
  public ThemisScan() {
    this(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
  }
  
  protected ThemisScan(Scan scan) {
    setHBaseScan(scan);
  }
  
  public ThemisScan(byte[] startRow, byte[] stopRow) {
    this.scan = new Scan(startRow, stopRow);
  }
  
  protected Scan getHBaseScan() {
    return this.scan;
  }
  
  protected void setHBaseScan(Scan scan) {
    this.scan = scan;
  }
  
  // must specify both the family and qualifier when add mutation
  @Override
  public ThemisScan addColumn(byte [] family, byte [] qualifier) throws IOException {
    checkContainingPreservedColumn(family, qualifier);
    this.scan.addColumn(family, qualifier);
    return this;
  }

  @Override
  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    return scan.getFamilyMap();
  }
  
  public ThemisScan setStartRow(byte [] startRow) {
    scan.setStartRow(startRow);
    return this;
  }
  
  public ThemisScan setStopRow(byte [] stopRow) {
    scan.setStopRow(stopRow);
    return this;
  }
  
  public int getCaching() {
    return scan.getCaching();
  }

  // TODO : Now, we don't provide setBatch(int)
  public ThemisScan setCaching(int caching) {
    scan.setCaching(caching);
    return this;
  }

  public byte [] getStartRow() {
    return scan.getStartRow();
  }

  public byte [] getStopRow() {
    return scan.getStopRow();
  }

  @Override
  public void setCacheBlocks(boolean cacheBlocks) {
    this.scan.setCacheBlocks(cacheBlocks);
  }

  @Override
  public boolean getCacheBlocks() {
    return this.scan.getCacheBlocks();
  }

  @Override
  protected ThemisRead setFilterWithoutCheck(Filter filter) {
    this.scan.setFilter(filter);
    return this;
  }

  @Override
  public Filter getFilter() {
    return this.scan.getFilter();
  }
  
  // TODO(cuijianwei): support reverse scan
}
