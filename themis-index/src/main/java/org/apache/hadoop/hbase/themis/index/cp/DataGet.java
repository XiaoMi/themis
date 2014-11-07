package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;

import org.apache.hadoop.hbase.themis.ThemisGet;
import org.apache.hadoop.hbase.util.Bytes;

// avoid set rowkey in ThemisGet
public class DataGet extends ThemisGet {
  private static final byte[] THEMIS_INDEX_DATA_GET_FEEK_ROW = Bytes.toBytes("__themis.feek.row__");
  public DataGet() {
    super(THEMIS_INDEX_DATA_GET_FEEK_ROW);
  }
  
  @Override
  public DataGet addColumn(byte [] family, byte [] qualifier) throws IOException {
    super.addColumn(family, qualifier);
    return this;
  }
  
  @Override
  public DataGet addFamily(byte[] family) throws IOException {
    super.addFamily(family);
    return this;
  }  
}
