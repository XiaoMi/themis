package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil.CommitFamily;
import org.apache.hadoop.hbase.util.Bytes;
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
  
  @Test
  public void testCheckContainingPreservedColumns() {
    useCommitFamily(CommitFamily.SAME_WITH_DATA_FAMILY);
    Map<byte[], NavigableSet<byte[]>> familyMap = new HashMap<byte[], NavigableSet<byte[]>>();
    familyMap.put(FAMILY, new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR));
    familyMap.get(FAMILY).add(QUALIFIER);
    familyMap.put(ANOTHER_FAMILY, null);
    try {
      ThemisRead.checkContainingPreservedColumns(familyMap);
    } catch (IOException e) {
      Assert.fail();
    }
    familyMap.get(FAMILY).add(ColumnUtil.getPutColumn(new Column(FAMILY, QUALIFIER)).getQualifier());
    try {
      ThemisRead.checkContainingPreservedColumns(familyMap);
      Assert.fail();
    } catch (IOException e) {
    }
  }
  
  @Test
  public void testCheckContainingPreservedColumnsForCommitToDifferentFamily() {
    useCommitFamily(CommitFamily.DIFFERNT_FAMILY);
    Map<byte[], NavigableSet<byte[]>> familyMap = new HashMap<byte[], NavigableSet<byte[]>>();
    familyMap.put(FAMILY, new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR));
    familyMap.get(FAMILY).add(QUALIFIER);
    familyMap.put(ANOTHER_FAMILY, null);
    try {
      ThemisRead.checkContainingPreservedColumns(familyMap);
    } catch (IOException e) {
      Assert.fail();
    }
    Column putColumn = ColumnUtil.getPutColumn(new Column(FAMILY, QUALIFIER));
    familyMap.put(putColumn.getFamily(), new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR));
    familyMap.get(putColumn.getFamily()).add(QUALIFIER);
    try {
      ThemisRead.checkContainingPreservedColumns(familyMap);
      Assert.fail();
    } catch (IOException e) {
    }
  }
}
