
package org.apache.hadoop.hbase.themis;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.Filter;

//a wrapper class of Get in HBase which not expose timestamp to user
public class ThemisGet extends ThemisRead {
  private Get get;
  
  public ThemisGet(byte[] row) {
    this.get = new Get(row);
  }
  
  protected ThemisGet(Get get) {
    setHBaseGet(get);
  }
  
  public byte[] getRow() {
    return this.get.getRow();
  }
  
  protected Get getHBaseGet() {
    return get;
  }
  
  protected void setHBaseGet(Get get) {
    this.get = get;
  }

  // must specify both the family and qualifier when querying
  @Override
  public ThemisGet addColumn(byte [] family, byte [] qualifier) throws IOException {
    checkContainingPreservedColumn(family, qualifier);
    this.get.addColumn(family, qualifier);
    return this;
  }
  
  @Override
  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    return get.getFamilyMap();
  }

  @Override
  public void setCacheBlocks(boolean cacheBlocks) {
    get.setCacheBlocks(cacheBlocks);
  }

  @Override
  public boolean getCacheBlocks() {
    return get.getCacheBlocks();
  }

  @Override
  public Filter getFilter() {
    return get.getFilter();
  }

  @Override
  protected ThemisRead setFilterWithoutCheck(Filter filter) {
    get.setFilter(filter);
    return this;
  }
}
