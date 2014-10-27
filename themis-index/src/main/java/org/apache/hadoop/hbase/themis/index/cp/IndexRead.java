package org.apache.hadoop.hbase.themis.index.cp;

import org.apache.hadoop.hbase.themis.ThemisScan;

public class IndexRead extends ThemisScan {
  protected final IndexColumn indexColumn;
  protected final DataGet dataGet;
  
  public IndexRead(IndexColumn indexColumn, ThemisScan indexScan, DataGet dataGet) {
    super.setHBaseScan(indexScan.getInternalScan());
    this.indexColumn = indexColumn;
    this.dataGet = dataGet;
  }
  
  public ThemisScan getIndexScan() {
    return this;
  }
  
  public DataGet getDataGet() {
    return dataGet;
  }
  
  public IndexColumn getIndexColumn() {
    return indexColumn;
  }
}
