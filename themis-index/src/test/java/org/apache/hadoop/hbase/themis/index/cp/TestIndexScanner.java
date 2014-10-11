package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.junit.Assert;
import org.junit.Test;

public class TestIndexScanner extends TestBase {
  @Test
  public void testConstructDataRowGet() throws IOException {
    ThemisGet get = new ThemisGet(ROW);
    get.addFamily(FAMILY);
    get.addColumn(ANOTHER_FAMILY, QUALIFIER);
    get.setFilter(new PrefixFilter(ROW));
    get.setCacheBlocks(true);
    
    ThemisGet actual = IndexScanner.constructDataRowGet(ANOTHER_ROW, get);
    Assert.assertArrayEquals(ANOTHER_ROW, actual.getRow());
    Assert
        .assertArrayEquals(QUALIFIER, actual.getFamilyMap().get(ANOTHER_FAMILY).iterator().next());
    Assert.assertTrue(actual.getFamilyMap().containsKey(FAMILY));
    Assert.assertNull(actual.getFamilyMap().get(FAMILY));
    Assert.assertTrue(actual.getCacheBlocks());
  }
}
