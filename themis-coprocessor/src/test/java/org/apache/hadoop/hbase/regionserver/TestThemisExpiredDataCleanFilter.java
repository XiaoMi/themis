package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TestThemisExpiredDataCleanFilter extends TransactionTestBase {
  protected ResultScanner getResultScanner(byte[] tableName, long cleanTs) throws IOException {
    Scan scan = new Scan();
    ThemisExpiredDataCleanFilter filter = new ThemisExpiredDataCleanFilter(cleanTs);
    scan.setFilter(filter);
    scan.setMaxVersions();
    return getTable(tableName).getScanner(scan);
  }
  
  @Test
  public void testThemisExpiredDataCleanFilter() throws IOException {
    // one row / one column
    writeData(COLUMN, prewriteTs);
    writeData(COLUMN, prewriteTs + 1);
    // won't clean
    ResultScanner scanner = getResultScanner(TABLENAME, prewriteTs);
    Result result = scanner.next();
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(prewriteTs + 1, result.list().get(0).getTimestamp());
    Assert.assertEquals(prewriteTs, result.list().get(1).getTimestamp());
    scanner.close();
    // won't clean
    scanner = getResultScanner(TABLENAME, prewriteTs + 1);
    result = scanner.next();
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(prewriteTs + 1, result.list().get(0).getTimestamp());
    Assert.assertEquals(prewriteTs, result.list().get(1).getTimestamp());
    scanner.close();
    // will clean one version
    scanner = getResultScanner(TABLENAME, prewriteTs + 2);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(prewriteTs + 1, result.list().get(0).getTimestamp());
    scanner.close();
    
    // one row / multi-columns
    writeData(COLUMN_WITH_ANOTHER_QUALIFIER, prewriteTs);
    writeData(COLUMN_WITH_ANOTHER_QUALIFIER, prewriteTs + 1);
    writeData(COLUMN_WITH_ANOTHER_FAMILY, prewriteTs);
    writeData(COLUMN_WITH_ANOTHER_FAMILY, prewriteTs + 1);
    scanner = getResultScanner(TABLENAME, prewriteTs + 2);
    result = scanner.next();
    scanner.close();
    Assert.assertEquals(3, result.size());
    ColumnCoordinate[] columns = new ColumnCoordinate[] { COLUMN_WITH_ANOTHER_FAMILY,
        COLUMN_WITH_ANOTHER_QUALIFIER, COLUMN };
    for (int i = 0; i < columns.length; ++i) {
      ColumnCoordinate column = columns[i];
      KeyValue kv = result.list().get(i);
      Assert.assertEquals(prewriteTs + 1, kv.getTimestamp());
      Assert.assertArrayEquals(column.getFamily(), kv.getFamily());
      Assert.assertArrayEquals(column.getQualifier(), kv.getQualifier());
    }
    
    // two row / multi-columns
    writeData(COLUMN_WITH_ANOTHER_ROW, prewriteTs);
    writeData(COLUMN_WITH_ANOTHER_ROW, prewriteTs + 1);
    scanner = getResultScanner(TABLENAME, prewriteTs + 2);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(prewriteTs + 1, result.list().get(0).getTimestamp());
    result = scanner.next();
    Assert.assertEquals(3, result.size());
    for (int i = 0; i < columns.length; ++i) {
      ColumnCoordinate column = columns[i];
      KeyValue kv = result.list().get(i);
      Assert.assertEquals(prewriteTs + 1, kv.getTimestamp());
      Assert.assertArrayEquals(column.getFamily(), kv.getFamily());
      Assert.assertArrayEquals(column.getQualifier(), kv.getQualifier());
    }
    scanner.close();
  }
}
