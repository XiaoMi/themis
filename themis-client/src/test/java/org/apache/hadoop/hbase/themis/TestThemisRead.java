package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.junit.Test;

public class TestThemisRead extends TestBase {
  @Test
  public void testSetFilter() throws IOException {
    ThemisRead[] readers = new ThemisRead[] { new ThemisGet(ROW), new ThemisScan() };
    for (ThemisRead reader : readers) {
      reader.setFilter(null);
      Assert.assertNull(reader.getFilter());
      
      // illegal Filter class directly
      try {
        reader.setFilter(new DependentColumnFilter());
        Assert.fail();
      } catch (IOException e) {}
      // illegal Filter class in FilterList
      FilterList filterList = new FilterList();
      filterList.addFilter(new PrefixFilter());
      filterList.addFilter(new ColumnRangeFilter());
      try {
        reader.setFilter(new DependentColumnFilter());
        Assert.fail();
      } catch (IOException e) {}
      
      reader.setFilter(new PrefixFilter());
      Assert.assertEquals(PrefixFilter.class.getName(), reader.getFilter().getClass().getName());
      filterList = new FilterList();
      filterList.addFilter(new PrefixFilter());
      reader.setFilter(filterList);
      Assert.assertEquals(FilterList.class.getName(), reader.getFilter().getClass().getName());
    }
  }
}
