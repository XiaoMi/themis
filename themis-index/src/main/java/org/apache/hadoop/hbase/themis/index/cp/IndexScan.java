package org.apache.hadoop.hbase.themis.index.cp;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.themis.ThemisScan;

public class IndexScan extends IndexRead {

  public IndexScan(IndexColumn indexColumn, ThemisScan indexScan, DataGet dataGet) {
    super(indexColumn, indexScan, dataGet);
  }

  public IndexScan(IndexColumn indexColumn, byte[] startRow, byte[] stopRow, DataGet dataGet) {
    this(indexColumn, new ThemisScan(startRow, stopRow), dataGet);
  }
  
  public IndexScan(IndexColumn indexColumn, byte[] startRow, byte[] stopRow) {
    this(indexColumn, new ThemisScan(startRow, stopRow), new DataGet());
  }
  
  public IndexScan(IndexColumn indexColumn) {
    this(indexColumn, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
  }
}
