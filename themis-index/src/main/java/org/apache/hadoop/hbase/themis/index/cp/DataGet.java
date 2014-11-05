package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;

import org.apache.hadoop.hbase.themis.ThemisGet;

// avoid set rowkey in ThemisGet
public class DataGet extends ThemisGet {
  public DataGet() {
    super((byte[])null);
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
