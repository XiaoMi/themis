package org.apache.hadoop.hbase.themis.index.cp;

import org.apache.hadoop.hbase.themis.ThemisScan;

public class IndexGet extends IndexRead {

  public IndexGet(IndexColumn indexColumn, ThemisScan indexScan, DataGet dataGet) {
    super(indexColumn, indexScan, dataGet);
  }
  
  public IndexGet(IndexColumn indexColumn, byte[] indexRow, DataGet dataGet) {
    this(indexColumn, new ThemisScan(indexRow), dataGet);
  }

  public IndexGet(IndexColumn indexColumn, byte[] indexRow) {
    this(indexColumn, new ThemisScan(indexRow, indexRow), new DataGet());
  }
}
