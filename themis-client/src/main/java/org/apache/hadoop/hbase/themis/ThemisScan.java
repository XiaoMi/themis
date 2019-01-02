package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ClientUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

//a wrapper class of Scan in HBase which not expose timestamp to user
public class ThemisScan extends ThemisRead {
  private Scan scan;

  public ThemisScan() {
    this(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
  }

  public ThemisScan(Scan scan) throws IOException {
    checkContainingPreservedColumns(scan.getFamilyMap());
    setHBaseScan(scan);
  }

  public ThemisScan(byte[] startRow, byte[] stopRow) {
    if (ClientUtil.areScanStartRowAndStopRowEqual(startRow, stopRow)) {
      // for keeping the old behavior that a scan with the same start and stop row is a get scan.
      this.scan = new Scan().withStartRow(startRow).withStopRow(stopRow, true);
    } else {
      this.scan = new Scan().withStartRow(startRow).withStopRow(stopRow);
    }
  }

  public ThemisScan(byte[] startRow) {
    this(startRow, HConstants.EMPTY_END_ROW);
  }

  protected Scan getHBaseScan() {
    return this.scan;
  }

  protected void setHBaseScan(Scan scan) {
    this.scan = scan;
  }

  // must specify both the family and qualifier when add mutation
  @Override
  public ThemisScan addColumn(byte[] family, byte[] qualifier) throws IOException {
    checkContainingPreservedColumn(family, qualifier);
    this.scan.addColumn(family, qualifier);
    return this;
  }

  @Override
  public ThemisScan addFamily(byte[] family) throws IOException {
    checkContainingPreservedColumn(family, null);
    this.scan.addFamily(family);
    return this;
  }

  @Override
  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    return scan.getFamilyMap();
  }

  public ThemisScan setStartRow(byte[] startRow) {
    scan.withStartRow(startRow);
    return this;
  }

  public ThemisScan setStopRow(byte[] stopRow) {
    if (ClientUtil.areScanStartRowAndStopRowEqual(scan.getStartRow(), stopRow)) {
      // for keeping the old behavior that a scan with the same start and stop row is a get scan.
      scan.withStopRow(stopRow, true);
    } else {
      scan.withStopRow(stopRow);
    }
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

  public byte[] getStartRow() {
    return scan.getStartRow();
  }

  public byte[] getStopRow() {
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

  public Scan getInternalScan() {
    return this.scan;
  }

  // TODO(cuijianwei): support reverse scan
}
