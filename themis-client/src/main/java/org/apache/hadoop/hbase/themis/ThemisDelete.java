package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;

//a wrapper class of Delete in HBase which not expose timestamp to user
public class ThemisDelete extends ThemisMutation {
  private Delete delete;
  
  public ThemisDelete(byte[] row) {
    this.delete = new Delete(row);
  }
  
  // TODO : check delete is legal for themis
  public ThemisDelete(Delete delete) {
    setHBaseDelete(delete);
  }
  
  public byte[] getRow() {
    return this.delete.getRow();
  }
  
  // must specify both the family and qualifier when add mutation
  public ThemisDelete deleteColumn(byte [] family, byte [] qualifier) throws IOException {
    checkContainingPreservedColumn(family, qualifier);
    this.delete.deleteColumns(family, qualifier, Long.MIN_VALUE);
    return this;
  }
  
  protected Delete getHBaseDelete() {
    return this.delete;
  }
  
  protected void setHBaseDelete(Delete delete) {
    this.delete = delete;
  }

  @Override
  public Map<byte[], List<KeyValue>> getFamilyMap() {
    return delete.getFamilyMap();
  }
}