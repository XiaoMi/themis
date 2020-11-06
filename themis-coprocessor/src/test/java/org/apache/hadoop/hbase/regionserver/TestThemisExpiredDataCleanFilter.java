package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TestThemisExpiredDataCleanFilter extends TransactionTestBase {
  protected ResultScanner getResultScanner(byte[] tableName, long cleanTs)
      throws IOException {
    return getTable(tableName).getScanner(getScan(cleanTs, false));
  }
  
  protected Scan getScan(long cleanTs, boolean enableDelete) throws IOException {
    Scan scan = new Scan();
    ThemisExpiredDataCleanFilter filter = enableDelete ? new ThemisExpiredDataCleanFilter(cleanTs,
        getRegion()) : new ThemisExpiredDataCleanFilter(cleanTs);
    scan.setFilter(filter);
    scan.setMaxVersions();
    return scan;
  }
  
  public HRegion getRegion() throws IOException {
    try {
      return TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME))
              .getOnlineRegions().get(0);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
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
    Assert.assertEquals(prewriteTs + 1, result.listCells().get(0).getTimestamp());
    Assert.assertEquals(prewriteTs, result.listCells().get(1).getTimestamp());
    scanner.close();
    // won't clean
    scanner = getResultScanner(TABLENAME, prewriteTs + 1);
    result = scanner.next();
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(prewriteTs + 1, result.listCells().get(0).getTimestamp());
    Assert.assertEquals(prewriteTs, result.listCells().get(1).getTimestamp());
    scanner.close();
    // will clean one version
    scanner = getResultScanner(TABLENAME, prewriteTs + 2);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(prewriteTs + 1, result.listCells().get(0).getTimestamp());
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
      Cell kv = result.listCells().get(i);
      Assert.assertEquals(prewriteTs + 1, kv.getTimestamp());
      Assert.assertArrayEquals(column.getFamily(), kv.getFamilyArray());
      Assert.assertArrayEquals(column.getQualifier(), kv.getFamilyArray());
    }
    
    // two row / multi-columns
    writeData(COLUMN_WITH_ANOTHER_ROW, prewriteTs);
    writeData(COLUMN_WITH_ANOTHER_ROW, prewriteTs + 1);
    scanner = getResultScanner(TABLENAME, prewriteTs + 2);
    result = scanner.next();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(prewriteTs + 1, result.listCells().get(0).getTimestamp());
    result = scanner.next();
    Assert.assertEquals(3, result.size());
    for (int i = 0; i < columns.length; ++i) {
      ColumnCoordinate column = columns[i];
      Cell kv = result.listCells().get(i);
      Assert.assertEquals(prewriteTs + 1, kv.getTimestamp());
      Assert.assertArrayEquals(column.getFamily(), kv.getFamilyArray());
      Assert.assertArrayEquals(column.getQualifier(), kv.getFamilyArray());
    }
    scanner.close();
  }
  
  @Test
  public void testThemisExpiredDataCleanFilterEnableDelete() throws IOException {
    // one row / one column
    writeData(getDeleteColumnCoordinate(COLUMN), prewriteTs);
    writeData(getDeleteColumnCoordinate(COLUMN), prewriteTs + 1);
    // won't clean
    RegionScanner scanner = getRegion().getScanner(getScan(prewriteTs, true));
    List<Cell> result = new ArrayList<>();
    scanner.next(result);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(prewriteTs + 1, result.get(0).getTimestamp());
    Assert.assertEquals(prewriteTs, result.get(1).getTimestamp());
    scanner.close();
    
    // will clean one version
    result.clear();
    scanner = getRegion().getScanner(getScan(prewriteTs + 1, true));
    scanner.next(result);
    scanner.close();
    result.clear();
    // the scanner won't see the delete because mvcc, test in next scan
    scanner = getRegion().getScanner(getScan(prewriteTs + 1, false));
    scanner.next(result);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(prewriteTs + 1, result.get(0).getTimestamp());
    result.clear();
    scanner.close();
    
    // will clean all
    scanner = getRegion().getScanner(getScan(prewriteTs + 2, true));
    scanner.next(result);
    Assert.assertEquals(1, result.size());
    result.clear();
    scanner.close();
    scanner = getRegion().getScanner(getScan(prewriteTs + 2, false));
    scanner.next(result);
    Assert.assertEquals(0, result.size());
    result.clear();
    scanner.close();
    
    // one row / multi-columns
    writeData(getDeleteColumnCoordinate(COLUMN_WITH_ANOTHER_QUALIFIER), prewriteTs);
    writeData(getDeleteColumnCoordinate(COLUMN_WITH_ANOTHER_QUALIFIER), prewriteTs + 1);
    writeData(getDeleteColumnCoordinate(COLUMN_WITH_ANOTHER_FAMILY), prewriteTs);
    writeData(getDeleteColumnCoordinate(COLUMN_WITH_ANOTHER_FAMILY), prewriteTs + 1);
    scanner = getRegion().getScanner(getScan(prewriteTs + 2, true));
    scanner.next(result);
    Assert.assertEquals(2, result.size());
    result.clear();
    scanner.close();
    
    scanner = getRegion().getScanner(getScan(prewriteTs + 2, false));
    scanner.next(result);
    Assert.assertEquals(0, result.size());
    result.clear();
    scanner.close();
    
    nextTransactionTs();
    
    // two row / multi-columns
    writeData(getDeleteColumnCoordinate(COLUMN), prewriteTs);
    writeData(getDeleteColumnCoordinate(COLUMN), prewriteTs + 1);
    writeData(getDeleteColumnCoordinate(COLUMN_WITH_ANOTHER_ROW), prewriteTs);
    writeData(getDeleteColumnCoordinate(COLUMN_WITH_ANOTHER_ROW), prewriteTs + 1);
    scanner = getRegion().getScanner(getScan(prewriteTs + 2, true));
    scanner.next(result);
    Assert.assertEquals(1, result.size());
    result.clear();
    scanner.next(result);
    Assert.assertEquals(1, result.size());
    result.clear();
    scanner.close();
    
    scanner = getRegion().getScanner(getScan(prewriteTs + 2, false));
    scanner.next(result);
    Assert.assertEquals(0, result.size());
    scanner.close();
  }
}
