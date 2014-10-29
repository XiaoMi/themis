package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.themis.cp.ThemisCpUtil;
import org.apache.hadoop.hbase.themis.cp.ThemisCpUtil.FilterCallable;

// abstract class for ThemisGet/Scan
public abstract class ThemisRead extends ThemisRequest {
  protected boolean hasColumn() {
    return getFamilyMap() != null && getFamilyMap().size() != 0;
  }

  public abstract Map<byte [], NavigableSet<byte []>> getFamilyMap(); 

  public abstract ThemisRead addColumn(byte [] family, byte [] qualifier) throws IOException;
  
  public abstract ThemisRead addFamily(byte[] family) throws IOException;
  
  public abstract void setCacheBlocks(boolean cacheBlocks);
  
  public abstract boolean getCacheBlocks();

  // themis does not expose timestamp and versions to users. Therefore, we disallowed
  // Filters in HBase which refer to timestamp or column match
  public ThemisRead setFilter(Filter filter) throws IOException {
    ThemisCpUtil.processFilters(filter, new FilterCallable() {
      public void processConcreteFilter(Filter filter) throws IOException {
        Class<? extends Filter> filterCls = filter.getClass();
        if (ThemisCpUtil.DISALLOWD_FILTERS.contains(filterCls)) {
          throw new IOException("themis read disallow this filterm, all disallowed filters : "
              + ThemisCpUtil.getDisallowedFilterClassNameString());
        }
      }
    });
    return setFilterWithoutCheck(filter);
  }
  
  protected abstract ThemisRead setFilterWithoutCheck(Filter filter);
  
  public abstract Filter getFilter();
  
  public static void checkContainingPreservedColumns(Map<byte[], NavigableSet<byte[]>> familyMap)
      throws IOException {
    for (Entry<byte[], NavigableSet<byte []>> entry : familyMap.entrySet()) {
      byte[] family = entry.getKey();
      if (entry.getValue() != null) {
        for (byte[] qualifier : entry.getValue()) {
          checkContainingPreservedColumn(family, qualifier);
        }
      }
    }
  }
}