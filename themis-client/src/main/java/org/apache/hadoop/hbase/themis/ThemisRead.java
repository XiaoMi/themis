package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.Map;
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
  
  public abstract void setCacheBlocks(boolean cacheBlocks);
  
  public abstract boolean getCacheBlocks();

  // themis does not expose timestamp to users and queried family/qualifer must
  // be set explicitly by users. Therefore, we only allowed use Filters in HBase which
  // not refer to timestamp and column match
  public ThemisRead setFilter(Filter filter) throws IOException {
    ThemisCpUtil.processFilters(filter, new FilterCallable() {
      public void processConcreteFilter(Filter filter) throws IOException {
        Class<? extends Filter> filterCls = filter.getClass();
        if (!(ThemisCpUtil.ALLOWED_ROWKEY_FILTER_CLASSES.contains(filterCls)
            || ThemisCpUtil.ALLOWED_COLUMN_FILTER_CLASSES.contains(filterCls)
            || ThemisCpUtil.ALLOWED_TRANSFER_FILTER_CLASSES.contains(filterCls))) {
          throw new IOException("any filter class must be one of the following classes : "
              + ThemisCpUtil.getAllowedFilterClassNameString());
        }
      }
    });
    return setFilterWithoutCheck(filter);
  }
  
  protected abstract ThemisRead setFilterWithoutCheck(Filter filter);
  
  public abstract Filter getFilter();
}