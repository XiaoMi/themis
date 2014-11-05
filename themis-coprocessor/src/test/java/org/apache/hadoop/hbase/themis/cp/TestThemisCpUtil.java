package org.apache.hadoop.hbase.themis.cp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.ThemisCpUtil.FilterCallable;
import org.apache.hadoop.hbase.themis.cp.ThemisCpUtil.RowLevelFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

public class TestThemisCpUtil extends TestBase {
  @Test
  public void testGetAllowedFilterClassNamesString() {
    String nameString = ThemisCpUtil.getDisallowedFilterClassNameString();
    System.out.println("Disallowed filter class name:" + nameString);
  }
  

  static class FiltersCounter extends FilterCallable {
    int concreteFilterCount = 0;
    public void processConcreteFilter(Filter filter) throws IOException {
      ++concreteFilterCount;
    }
  }
  
  static class ConcreteFiltersCounter extends FiltersCounter {
    public boolean processFilterListOperator(Operator op) {
      return false;
    }
  }
  
  @Test
  public void testToConcreteFilters() throws IOException {
    FiltersCounter counter = new FiltersCounter();
    ThemisCpUtil.processFilters(null, counter);
    Assert.assertEquals(0, counter.concreteFilterCount);
    counter = new FiltersCounter();
    ThemisCpUtil.processFilters(new ColumnRangeFilter(ANOTHER_QUALIFIER, true, QUALIFIER, true), counter);
    Assert.assertEquals(1, counter.concreteFilterCount);
    counter = new FiltersCounter();
    FilterList filterList = new FilterList();
    filterList.addFilter(new ColumnRangeFilter(ANOTHER_QUALIFIER, true, QUALIFIER, true));
    filterList.addFilter(new ColumnCountGetFilter(1));
    ThemisCpUtil.processFilters(filterList, counter);
    Assert.assertEquals(2, counter.concreteFilterCount);
    // test process filterlist operator
    counter = new ConcreteFiltersCounter();
    ThemisCpUtil.processFilters(new ColumnRangeFilter(ANOTHER_QUALIFIER, true, QUALIFIER, true), counter);
    Assert.assertEquals(1, counter.concreteFilterCount);
    counter = new ConcreteFiltersCounter();
    ThemisCpUtil.processFilters(filterList, counter);
    Assert.assertEquals(0, counter.concreteFilterCount);
  }
  
  public static class CustomerRowkeyFilter extends FilterBase implements RowLevelFilter {
    private byte[] rowkey;
    
    public CustomerRowkeyFilter() {}
    
    public CustomerRowkeyFilter(byte[] rowkey) {
      this.rowkey = rowkey;
    }
    
    public boolean filterRowKey(byte [] buffer, int offset, int length) {
      return !Bytes.equals(rowkey, 0, rowkey.length, buffer, offset, length);
    }
    
    public void readFields(DataInput in) throws IOException {
      rowkey = Bytes.readByteArray(in);
    }

    public void write(DataOutput out) throws IOException {
      Bytes.writeByteArray(out, rowkey);
    }

    public boolean isRowLevelFilter() {
      return true;
    }
  }
  
  public static class CustomerColumnFilter extends FilterBase implements RowLevelFilter {
    private byte[] qualifier;
    
    public CustomerColumnFilter() {}
    
    public CustomerColumnFilter(byte[] qualifier) {
      this.qualifier = qualifier;
    }

    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      if (Bytes.equals(qualifier, cell.getQualifier())) {
        return ReturnCode.INCLUDE;
      }
      return ReturnCode.SKIP;
    }
    
    public void readFields(DataInput in) throws IOException {
      qualifier = Bytes.readByteArray(in);
    }

    public void write(DataOutput out) throws IOException {
      Bytes.writeByteArray(out, qualifier);
    }

    public boolean isRowLevelFilter() {
      return false;
    }
  }
  
  @Test
  public void testMoveRowkeyFiltersForWriteColumnGet() throws IOException {
    Get sourceGet = new Get(ROW);
    Get dstGet = new Get(ROW);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    Assert.assertNull(sourceGet.getFilter());
    Assert.assertNull(dstGet.getFilter());
    dstGet.setFilter(null);
    // can not move filter, no rowkey filter
    Filter expected = new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOp.EQUAL, VALUE);
    sourceGet.setFilter(expected);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    Assert.assertEquals(expected, sourceGet.getFilter());
    Assert.assertNull(dstGet.getFilter());
    dstGet.setFilter(null);
    // can not move filter, has operation=MUST_PASS_ONE
    FilterList filters = new FilterList(Operator.MUST_PASS_ONE);
    filters.addFilter(new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOp.EQUAL, VALUE));
    filters.addFilter(new PrefixFilter(ROW));
    sourceGet.setFilter(filters);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    Assert.assertEquals(filters, sourceGet.getFilter());
    Assert.assertNull(dstGet.getFilter());
    dstGet.setFilter(null);
    // move filter, concrete rowkey filter
    expected = new PrefixFilter(ROW);
    sourceGet.setFilter(expected);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    Assert.assertEquals(0, ((FilterList)sourceGet.getFilter()).getFilters().size());
    Assert.assertEquals(1, ((FilterList)dstGet.getFilter()).getFilters().size());
    Assert.assertEquals(expected, ((FilterList)dstGet.getFilter()).getFilters().get(0));
    dstGet.setFilter(null);
    // move filter, filterlist with MUST_PASS_ALL
    filters = new FilterList();
    filters.addFilter(new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOp.EQUAL, VALUE));
    filters.addFilter(new PrefixFilter(ROW));
    sourceGet.setFilter(filters);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    FilterList sourceFilter = (FilterList)sourceGet.getFilter();
    FilterList dstFilter = (FilterList)dstGet.getFilter();
    Assert.assertEquals(1, sourceFilter.getFilters().size());
    Assert.assertTrue(sourceFilter.getFilters().get(0) instanceof SingleColumnValueFilter);
    Assert.assertEquals(1, dstFilter.getFilters().size());
    Assert.assertTrue(dstFilter.getFilters().get(0) instanceof PrefixFilter);
    dstGet.setFilter(null);
    // test customer filters
    filters = new FilterList();
    filters.addFilter(new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOp.EQUAL, VALUE));
    filters.addFilter(new PrefixFilter(ROW));
    filters.addFilter(new CustomerRowkeyFilter(ROW));
    filters.addFilter(new CustomerColumnFilter(QUALIFIER));
    sourceGet.setFilter(filters);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    sourceFilter = (FilterList)sourceGet.getFilter();
    dstFilter = (FilterList)dstGet.getFilter();
    Assert.assertEquals(2, sourceFilter.getFilters().size());
    for (Filter filter : sourceFilter.getFilters()) {
      Assert.assertFalse(filter instanceof PrefixFilter);
      Assert.assertFalse(filter instanceof CustomerRowkeyFilter);
    }
    Assert.assertEquals(2, dstFilter.getFilters().size());
    for (Filter filter : dstFilter.getFilters()) {
      Assert.assertFalse(filter instanceof SingleColumnValueFilter);
      Assert.assertFalse(filter instanceof CustomerColumnFilter);
    }
    dstGet.setFilter(null);
    
    // test destGet with exist Filters
    filters = new FilterList();
    filters.addFilter(new SingleColumnValueFilter(null, null, null, (byte[])null));
    filters.addFilter(new PrefixFilter(null));
    sourceGet.setFilter(filters);
    dstGet.setFilter(new ExcludeDataColumnFilter());
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    sourceFilter = (FilterList)sourceGet.getFilter();
    dstFilter = (FilterList)dstGet.getFilter();
    Assert.assertEquals(2, dstFilter.getFilters().size());
    Assert.assertTrue((dstFilter.getFilters().get(0) instanceof ExcludeDataColumnFilter)
        || (dstFilter.getFilters().get(1) instanceof ExcludeDataColumnFilter));
    dstGet.setFilter(null);
  }
  
  protected void checkConstructedDataGet(List<KeyValue> putKvs, Filter filter, Get get) {
    Assert.assertEquals(putKvs.size(), get.getFamilyMap().size());
    for (KeyValue kv : putKvs) {
      Assert.assertTrue(get.getFamilyMap().containsKey(kv.getFamily()));
      get.getTimeRange().withinTimeRange(kv.getTimestamp());
    }
    if (filter == null) {
      Assert.assertTrue(get.getFilter() instanceof ColumnTimestampFilter);
    } else {
      Assert.assertTrue(get.getFilter() instanceof FilterList);
      FilterList filterList = (FilterList)get.getFilter();
      Assert.assertEquals(2, filterList.getFilters().size());
      Assert.assertTrue(filterList.getFilters().get(0) instanceof ColumnTimestampFilter);
      Assert.assertEquals(filter, filterList.getFilters().get(1));
    }
  }
  
  @Test
  public void testConstructDataGetByWriteColumnKvs() throws IOException {
    List<KeyValue> putKvs = new ArrayList<KeyValue>();
    putKvs.add(getPutKv(COLUMN, PREWRITE_TS, COMMIT_TS));
    putKvs.add(getPutKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS + 10 , COMMIT_TS + 10));
    Get get = ThemisCpUtil.constructDataGetByPutKvs(putKvs, null);
    checkConstructedDataGet(putKvs, null, get);
    Filter filter = new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOp.EQUAL, VALUE);
    get = ThemisCpUtil.constructDataGetByPutKvs(putKvs, filter);
    checkConstructedDataGet(putKvs, filter, get);
  }
  
  @Test
  public void testGetPutKvs() {
    List<KeyValue> writeKvs = new ArrayList<KeyValue>();
    writeKvs.add(getPutKv(COLUMN, PREWRITE_TS));
    writeKvs.add(getDeleteKv(COLUMN, PREWRITE_TS + 1));
    writeKvs.add(getPutKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS + 1));
    writeKvs.add(getDeleteKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS));
    writeKvs.add(getDeleteKv(new ColumnCoordinate(ROW, ANOTHER_FAMILY, ANOTHER_QUALIFIER), PREWRITE_TS));
    writeKvs.add(getPutKv(COLUMN_WITH_ANOTHER_QUALIFIER, PREWRITE_TS));
    Collections.sort(writeKvs, KeyValue.COMPARATOR);
    List<KeyValue> putKvs = ThemisCpUtil.getPutKvs(writeKvs);
    Assert.assertEquals(2, putKvs.size());
    Collections.sort(putKvs, new KVComparator());
    Assert.assertTrue(getPutKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS + 1).equals(putKvs.get(0)));
    Assert.assertTrue(getPutKv(COLUMN_WITH_ANOTHER_QUALIFIER, PREWRITE_TS).equals(putKvs.get(1)));
    
    // test two different familes with different values
    writeKvs.clear();
    writeKvs.add(getPutKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS));
    writeKvs.add(getPutKv(COLUMN, PREWRITE_TS));
    Collections.sort(writeKvs, new KVComparator());
    putKvs = ThemisCpUtil.getPutKvs(writeKvs);
    Assert.assertTrue(getPutKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS).equals(putKvs.get(0)));
    Assert.assertTrue(getPutKv(COLUMN, PREWRITE_TS).equals(putKvs.get(1)));
  }
  
  @Test
  public void testCreateScanWithLockAndWriteColumns() throws IOException {
    byte[] startRow = Bytes.toBytes("aaa");
    byte[] stopRow = Bytes.toBytes("bbb");
    Scan scan = new Scan(startRow, stopRow);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.addColumn(ANOTHER_FAMILY, ANOTHER_QUALIFIER);
    Scan createdScan = ThemisCpUtil.constructLockAndWriteScan(scan, PREWRITE_TS);
    Assert.assertArrayEquals(startRow, createdScan.getStartRow());
    Assert.assertArrayEquals(stopRow, createdScan.getStopRow());
    Assert.assertEquals(3, createdScan.getFamilies().length);
    checkReadWithLockAndWriteColumns(createdScan.getFamilyMap(), COLUMN);
    checkReadWithLockAndWriteColumns(createdScan.getFamilyMap(),
      new ColumnCoordinate(ANOTHER_FAMILY, ANOTHER_QUALIFIER));
    
    // test scan family
    scan = new Scan(startRow, stopRow);
    scan.addFamily(FAMILY);
    scan.addColumn(ANOTHER_FAMILY, QUALIFIER);
    createdScan = ThemisCpUtil.constructLockAndWriteScan(scan, PREWRITE_TS);
    Assert.assertArrayEquals(startRow, createdScan.getStartRow());
    Assert.assertArrayEquals(stopRow, createdScan.getStopRow());
    Assert.assertEquals(3, createdScan.getFamilies().length);
    checkReadWithWriteColumns(createdScan.getFamilyMap(), new ColumnCoordinate(ANOTHER_FAMILY,
        QUALIFIER));
    Assert.assertTrue(createdScan.getFamilyMap().containsKey(FAMILY)
        && createdScan.getFamilyMap().get(FAMILY) == null);
    Assert.assertTrue(createdScan.getFamilyMap().containsKey(ColumnUtil.LOCK_FAMILY_NAME)
      && createdScan.getFamilyMap().get(ColumnUtil.LOCK_FAMILY_NAME) == null);
    Assert.assertTrue(createdScan.getFilter() instanceof ExcludeDataColumnFilter);
  }
  
  @Test
  public void testAddLockAndWriteColumnToGet() throws IOException {
    byte[][] families = new byte[][] {FAMILY, FAMILY, ANOTHER_FAMILY};
    byte[][] qualifiers = new byte[][] {QUALIFIER, ANOTHER_FAMILY, QUALIFIER};
    Get userGet = new Get(ROW);
    for (int i = 0; i < families.length; ++i) {
      userGet.addColumn(families[i], qualifiers[i]);
    }
    Get internalGet = new Get(userGet.getRow());
    ThemisCpUtil.addLockAndWriteColumnToGet(userGet, internalGet, PREWRITE_TS);
    Assert.assertEquals(3, internalGet.getFamilyMap().size());
    Assert.assertEquals(4, internalGet.getFamilyMap().get(FAMILY).size());
    Assert.assertEquals(2, internalGet.getFamilyMap().get(ANOTHER_FAMILY).size());
    Assert.assertEquals(3,
      internalGet.getFamilyMap().get(ColumnUtil.LOCK_FAMILY_NAME).size());
    for (int i = 0; i < families.length; ++i) {
      checkReadWithLockAndWriteColumns(internalGet.getFamilyMap(),
        new ColumnCoordinate(families[i], qualifiers[i]));
    }
    Assert.assertEquals(PREWRITE_TS, internalGet.getTimeRange().getMax());
    Assert.assertEquals(1, internalGet.getMaxVersions());
    
    // test for family-level transfer
    userGet = new Get(ROW);
    userGet.addFamily(FAMILY);
    internalGet = new Get(userGet.getRow());
    ThemisCpUtil.addLockAndWriteColumnToGet(userGet, internalGet, PREWRITE_TS);
    checkAddLockAndDataFamily(internalGet, ColumnUtil.LOCK_FAMILY_NAME, FAMILY);
    Assert.assertTrue(internalGet.getFilter() instanceof ExcludeDataColumnFilter);
    
    // test for combination of family-level and column-level transfer
    userGet = new Get(ROW);
    userGet.addFamily(FAMILY);
    userGet.addColumn(ANOTHER_FAMILY, ANOTHER_QUALIFIER);
    internalGet = new Get(userGet.getRow());
    ThemisCpUtil.addLockAndWriteColumnToGet(userGet, internalGet, PREWRITE_TS);
    checkAddLockAndDataFamily(internalGet, ColumnUtil.LOCK_FAMILY_NAME, FAMILY);
    checkReadWithWriteColumns(internalGet.getFamilyMap(), new ColumnCoordinate(ANOTHER_FAMILY,
        ANOTHER_QUALIFIER));
  }

  @Test
  public void testRemoveNotRequiredLockColumns() {
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    List<KeyValue> sourceKvs = new ArrayList<KeyValue>();
    sourceKvs.add(KEYVALUE);
    Column lockColumn = ColumnUtil.getLockColumn(FAMILY, QUALIFIER);
    KeyValue lockKv = new KeyValue(ROW, lockColumn.getFamily(), lockColumn.getQualifier(), PREWRITE_TS,
      Type.Put, VALUE);
    sourceKvs.add(lockKv);
    lockColumn = ColumnUtil.getLockColumn(ANOTHER_FAMILY, QUALIFIER);
    sourceKvs.add(new KeyValue(ROW, lockColumn.getFamily(), lockColumn.getQualifier(), PREWRITE_TS,
        Type.Put, VALUE));
    Result result = ThemisCpUtil.removeNotRequiredLockColumns(get.getFamilyMap(), new Result(
        sourceKvs));
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(KEYVALUE.equals(result.list().get(0)));
    Assert.assertTrue(lockKv.equals(result.list().get(1)));
  }
  
  protected static void checkAddLockAndDataFamily(Get internalGet, byte[]... families) {
    for (byte[] family : families) {
      Assert.assertTrue(internalGet.getFamilyMap().containsKey(family));
      Assert.assertNull(internalGet.getFamilyMap().get(family));
    }
    Assert.assertTrue(internalGet.getFilter() instanceof ExcludeDataColumnFilter);
    
  }
  
  @Test
  public void testAddWriteColumnToGet() {
    Get get = new Get(COLUMN.getRow());
    ThemisCpUtil.addWriteColumnToGet(COLUMN, get);
    checkReadWithWriteColumns(get.getFamilyMap(), COLUMN);
  }
  
  public static void checkReadWithLockAndWriteColumns(Map<byte[], NavigableSet<byte[]>> families,
      ColumnCoordinate columnCoordinate) {
    Column lockColumn = ColumnUtil.getLockColumn(columnCoordinate);
    Assert.assertTrue(families.get(lockColumn.getFamily()).contains(lockColumn.getQualifier()));
    checkReadWithWriteColumns(families, columnCoordinate);
  }
  
  public static void checkReadWithWriteColumns(Map<byte[], NavigableSet<byte[]>> families,
      ColumnCoordinate columnCoordinate) {
    Column putColumn = ColumnUtil.getPutColumn(columnCoordinate);
    Assert.assertTrue(families.get(putColumn.getFamily()).contains(putColumn.getQualifier()));
    Column delColumn = ColumnUtil.getDeleteColumn(columnCoordinate);
    Assert.assertTrue(families.get(delColumn.getFamily()).contains(delColumn.getQualifier()));
  }
  
  @Test
  public void testAddLockAndWriteColumnToScan() {
    Scan scan = new Scan();
    ThemisCpUtil.addLockAndWriteColumnToScan(COLUMN, scan);
    checkReadWithLockAndWriteColumns(scan.getFamilyMap(), COLUMN);
  }
  
  @Test
  public void testIsLockResult() {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(KEYVALUE);
    Assert.assertFalse(ThemisCpUtil.isLockResult(new Result(kvs)));
    kvs.add(getLockKv(KEYVALUE));
    Assert.assertFalse(ThemisCpUtil.isLockResult(new Result(kvs)));
    kvs.clear();
    kvs.add(getLockKv(KEYVALUE));
    Assert.assertTrue(ThemisCpUtil.isLockResult(new Result(kvs)));
    kvs.add(KEYVALUE);
    Assert.assertTrue(ThemisCpUtil.isLockResult(new Result(kvs)));
  }
  
  @Test
  public void testSeperateLockAndWriteKvs() {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(KEYVALUE);
    kvs.add(getLockKv(KEYVALUE));
    kvs.add(getPutKv(KEYVALUE));
    Column column = ColumnUtil.getDeleteColumn(new Column(FAMILY, QUALIFIER));
    KeyValue deleteKv = new KeyValue(ROW, column.getFamily(), column.getQualifier(), PREWRITE_TS, Type.Put, VALUE);
    kvs.add(deleteKv);
    Pair<List<KeyValue>, List<KeyValue>> result = ThemisCpUtil.seperateLockAndWriteKvs(kvs);
    Assert.assertEquals(1, result.getFirst().size());
    Assert.assertTrue(getLockKv(KEYVALUE).equals(result.getFirst().get(0)));
    Assert.assertEquals(2, result.getSecond().size());
    Assert.assertTrue(getPutKv(KEYVALUE).equals(result.getSecond().get(0)));
    Assert.assertTrue(deleteKv.equals(result.getSecond().get(1)));
    
    result = ThemisCpUtil.seperateLockAndWriteKvs(null);
    Assert.assertEquals(0, result.getFirst().size());
    Assert.assertEquals(0, result.getSecond().size());
  }
}
