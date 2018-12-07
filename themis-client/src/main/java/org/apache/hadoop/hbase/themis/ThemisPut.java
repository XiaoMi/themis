package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;

// a wrapper class of Put in HBase which not expose timestamp to user
public class ThemisPut extends ThemisMutation {
  private Put put;

  public ThemisPut(byte[] row) {
    put = new Put(row);
  }

  public ThemisPut(Put put) throws IOException {
    checkContainingPreservedColumns(put.getFamilyCellMap());
    setHBasePut(put);
  }

  public byte[] getRow() {
    return put.getRow();
  }

  // must specify both the family and qualifier when add mutation
  public ThemisPut add(byte[] family, byte[] qualifier, byte[] value) throws IOException {
    checkContainingPreservedColumn(family, qualifier);
    this.put.addColumn(family, qualifier, 0l, value);
    return this;
  }

  protected Put getHBasePut() {
    return this.put;
  }

  protected void setHBasePut(Put put) {
    this.put = put;
  }

  @Override
  public Map<byte[], List<Cell>> getFamilyMap() {
    return put.getFamilyCellMap();
  }
}
