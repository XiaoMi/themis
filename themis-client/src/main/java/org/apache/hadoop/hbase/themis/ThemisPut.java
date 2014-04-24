package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;

// a wrapper class of Put in HBase which not expose timestamp to user
public class ThemisPut extends ThemisMutation {
  private Put put;
  
  public ThemisPut(byte[] row) {
    put = new Put(row);
  }
  
  protected ThemisPut(Put put) {
    setHBasePut(put);
  }

  public byte[] getRow() {
    return put.getRow();
  }

  // must specify both the family and qualifier when add mutation
  public ThemisPut add(byte[] family, byte[] qualifier, byte[] value) throws IOException {
    checkContainingPreservedColumn(family, qualifier);
    this.put.add(family, qualifier, Long.MIN_VALUE, value);
    return this;
  }
  
  protected Put getHBasePut() {
    return this.put;
  }
  
  protected void setHBasePut(Put put) {
    this.put = put;
  }

  @Override
  protected boolean hasColumn() {
    return put.getFamilyMap() != null && put.getFamilyMap().size() != 0;
  }

  @Override
  public Map<byte[], List<KeyValue>> getFamilyMap() {
    return put.getFamilyMap();
  }
}
