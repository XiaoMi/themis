package org.apache.hadoop.hbase.themis.cp;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
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
    assertEquals(0, counter.concreteFilterCount);
    counter = new FiltersCounter();
    ThemisCpUtil.processFilters(new ColumnRangeFilter(ANOTHER_QUALIFIER, true, QUALIFIER, true),
      counter);
    assertEquals(1, counter.concreteFilterCount);
    counter = new FiltersCounter();
    FilterList filterList = new FilterList();
    filterList.addFilter(new ColumnRangeFilter(ANOTHER_QUALIFIER, true, QUALIFIER, true));
    filterList.addFilter(new ColumnCountGetFilter(1));
    ThemisCpUtil.processFilters(filterList, counter);
    assertEquals(2, counter.concreteFilterCount);
    // test process filterlist operator
    counter = new ConcreteFiltersCounter();
    ThemisCpUtil.processFilters(new ColumnRangeFilter(ANOTHER_QUALIFIER, true, QUALIFIER, true),
      counter);
    assertEquals(1, counter.concreteFilterCount);
    counter = new ConcreteFiltersCounter();
    ThemisCpUtil.processFilters(filterList, counter);
    assertEquals(0, counter.concreteFilterCount);
  }

  public static class CustomerRowkeyFilter extends FilterBase implements RowLevelFilter {
    private byte[] rowkey;

    public CustomerRowkeyFilter() {
    }

    public CustomerRowkeyFilter(byte[] rowkey) {
      this.rowkey = rowkey;
    }

    public boolean filterRowKey(byte[] buffer, int offset, int length) {
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

    public CustomerColumnFilter() {
    }

    public CustomerColumnFilter(byte[] qualifier) {
      this.qualifier = qualifier;
    }

    public ReturnCode filterKeyValue(Cell cell) throws IOException {
      if (Bytes.equals(qualifier, CellUtil.cloneQualifier(cell))) {
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
    assertNull(sourceGet.getFilter());
    assertNull(dstGet.getFilter());
    dstGet.setFilter(null);
    // can not move filter, no rowkey filter
    Filter expected = new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE);
    sourceGet.setFilter(expected);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    assertEquals(expected, sourceGet.getFilter());
    assertNull(dstGet.getFilter());
    dstGet.setFilter(null);
    // can not move filter, has operation=MUST_PASS_ONE
    FilterList filters = new FilterList(Operator.MUST_PASS_ONE);
    filters.addFilter(new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE));
    filters.addFilter(new PrefixFilter(ROW));
    sourceGet.setFilter(filters);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    assertEquals(filters, sourceGet.getFilter());
    assertNull(dstGet.getFilter());
    dstGet.setFilter(null);
    // move filter, concrete rowkey filter
    expected = new PrefixFilter(ROW);
    sourceGet.setFilter(expected);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    assertEquals(0, ((FilterList) sourceGet.getFilter()).getFilters().size());
    assertEquals(1, ((FilterList) dstGet.getFilter()).getFilters().size());
    assertEquals(expected, ((FilterList) dstGet.getFilter()).getFilters().get(0));
    dstGet.setFilter(null);
    // move filter, filterlist with MUST_PASS_ALL
    filters = new FilterList();
    filters.addFilter(new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE));
    filters.addFilter(new PrefixFilter(ROW));
    sourceGet.setFilter(filters);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    FilterList sourceFilter = (FilterList) sourceGet.getFilter();
    FilterList dstFilter = (FilterList) dstGet.getFilter();
    assertEquals(1, sourceFilter.getFilters().size());
    assertTrue(sourceFilter.getFilters().get(0) instanceof SingleColumnValueFilter);
    assertEquals(1, dstFilter.getFilters().size());
    assertTrue(dstFilter.getFilters().get(0) instanceof PrefixFilter);
    dstGet.setFilter(null);
    // test customer filters
    filters = new FilterList();
    filters.addFilter(new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE));
    filters.addFilter(new PrefixFilter(ROW));
    filters.addFilter(new CustomerRowkeyFilter(ROW));
    filters.addFilter(new CustomerColumnFilter(QUALIFIER));
    sourceGet.setFilter(filters);
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    sourceFilter = (FilterList) sourceGet.getFilter();
    dstFilter = (FilterList) dstGet.getFilter();
    assertEquals(2, sourceFilter.getFilters().size());
    for (Filter filter : sourceFilter.getFilters()) {
      assertFalse(filter instanceof PrefixFilter);
      assertFalse(filter instanceof CustomerRowkeyFilter);
    }
    assertEquals(2, dstFilter.getFilters().size());
    for (Filter filter : dstFilter.getFilters()) {
      assertFalse(filter instanceof SingleColumnValueFilter);
      assertFalse(filter instanceof CustomerColumnFilter);
    }
    dstGet.setFilter(null);

    // test destGet with exist Filters
    filters = new FilterList();
    filters
      .addFilter(new SingleColumnValueFilter(null, null, (CompareOperator) null, (byte[]) null));
    filters.addFilter(new PrefixFilter(null));
    sourceGet.setFilter(filters);
    dstGet.setFilter(new ExcludeDataColumnFilter());
    ThemisCpUtil.moveRowkeyFiltersForWriteGet(sourceGet, dstGet);
    sourceFilter = (FilterList) sourceGet.getFilter();
    dstFilter = (FilterList) dstGet.getFilter();
    assertEquals(2, dstFilter.getFilters().size());
    assertTrue((dstFilter.getFilters().get(0) instanceof ExcludeDataColumnFilter) ||
      (dstFilter.getFilters().get(1) instanceof ExcludeDataColumnFilter));
    dstGet.setFilter(null);
  }

  protected void checkConstructedDataGet(List<Cell> putKvs, Filter filter, Get get) {
    assertEquals(putKvs.size(), get.getFamilyMap().size());
    for (Cell kv : putKvs) {
      if (ColumnUtil.isCommitToSameFamily()) {
        assertTrue(get.getFamilyMap().containsKey(CellUtil.cloneFamily(kv)));
      }
      get.getTimeRange().withinTimeRange(kv.getTimestamp());
    }
    if (filter == null) {
      assertTrue(get.getFilter() instanceof ColumnTimestampFilter);
    } else {
      assertTrue(get.getFilter() instanceof FilterList);
      FilterList filterList = (FilterList) get.getFilter();
      assertEquals(2, filterList.getFilters().size());
      assertTrue(filterList.getFilters().get(0) instanceof ColumnTimestampFilter);
      assertEquals(filter, filterList.getFilters().get(1));
    }
  }

  @Test
  public void testConstructDataGetByWriteColumnKvs() throws IOException {
    List<Cell> putKvs = new ArrayList<>();
    putKvs.add(getPutKv(COLUMN, PREWRITE_TS, COMMIT_TS));
    putKvs.add(getPutKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS + 10, COMMIT_TS + 10));
    Get get = ThemisCpUtil.constructDataGetByPutKvs(putKvs, null);
    checkConstructedDataGet(putKvs, null, get);
    Filter filter = new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE);
    get = ThemisCpUtil.constructDataGetByPutKvs(putKvs, filter);
    checkConstructedDataGet(putKvs, filter, get);
  }

  private void assertCellEquals(Cell left, Cell right) {
    assertTrue(CellUtil.equals(left, right));
  }

  @Test
  public void testGetPutKvs() {
    List<Cell> writeKvs = new ArrayList<>();
    writeKvs.add(getPutKv(COLUMN, PREWRITE_TS));
    writeKvs.add(getDeleteKv(COLUMN, PREWRITE_TS + 1));
    writeKvs.add(getPutKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS + 1));
    writeKvs.add(getDeleteKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS));
    writeKvs
      .add(getDeleteKv(new ColumnCoordinate(ROW, ANOTHER_FAMILY, ANOTHER_QUALIFIER), PREWRITE_TS));
    writeKvs.add(getPutKv(COLUMN_WITH_ANOTHER_QUALIFIER, PREWRITE_TS));
    Collections.sort(writeKvs, CellComparator.getInstance());
    List<Cell> putKvs = ThemisCpUtil.getPutKvs(writeKvs);
    assertEquals(2, putKvs.size());
    Collections.sort(putKvs, CellComparator.getInstance());
    assertCellEquals(getPutKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS + 1), putKvs.get(0));
    assertCellEquals(getPutKv(COLUMN_WITH_ANOTHER_QUALIFIER, PREWRITE_TS), putKvs.get(1));

    // test two different familes with different values
    writeKvs.clear();
    writeKvs.add(getPutKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS));
    writeKvs.add(getPutKv(COLUMN, PREWRITE_TS));
    Collections.sort(writeKvs, CellComparator.getInstance());
    putKvs = ThemisCpUtil.getPutKvs(writeKvs);
    assertCellEquals(getPutKv(COLUMN_WITH_ANOTHER_FAMILY, PREWRITE_TS), putKvs.get(0));
    assertCellEquals(getPutKv(COLUMN, PREWRITE_TS), putKvs.get(1));
  }

  @Test
  public void testCreateScanWithLockAndWriteColumns() throws IOException {
    byte[] startRow = Bytes.toBytes("aaa");
    byte[] stopRow = Bytes.toBytes("bbb");
    Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);
    scan.addColumn(FAMILY, QUALIFIER);
    scan.addColumn(ANOTHER_FAMILY, ANOTHER_QUALIFIER);
    Scan createdScan = ThemisCpUtil.constructLockAndWriteScan(scan, PREWRITE_TS);
    assertArrayEquals(startRow, createdScan.getStartRow());
    assertArrayEquals(stopRow, createdScan.getStopRow());
    assertEquals(3, createdScan.getFamilies().length);
    checkReadWithLockAndWriteColumns(createdScan.getFamilyMap(), COLUMN);
    checkReadWithLockAndWriteColumns(createdScan.getFamilyMap(),
      new ColumnCoordinate(ANOTHER_FAMILY, ANOTHER_QUALIFIER));

    // test scan family
    scan = new Scan().withStartRow(startRow).withStopRow(stopRow);
    scan.addFamily(ANOTHER_FAMILY);
    scan.addColumn(FAMILY, QUALIFIER);
    createdScan = ThemisCpUtil.constructLockAndWriteScan(scan, PREWRITE_TS);
    assertArrayEquals(startRow, createdScan.getStartRow());
    assertArrayEquals(stopRow, createdScan.getStopRow());
    if (ColumnUtil.isCommitToSameFamily()) {
      assertEquals(3, createdScan.getFamilies().length);
      checkReadWithWriteColumns(createdScan.getFamilyMap(),
        new ColumnCoordinate(FAMILY, QUALIFIER));
    } else {
      assertEquals(4, createdScan.getFamilies().length);
    }
    assertTrue(createdScan.getFamilyMap().containsKey(ANOTHER_FAMILY) &&
      createdScan.getFamilyMap().get(ANOTHER_FAMILY) == null);
    assertTrue(createdScan.getFamilyMap().containsKey(ColumnUtil.LOCK_FAMILY_NAME) &&
      createdScan.getFamilyMap().get(ColumnUtil.LOCK_FAMILY_NAME) == null);
    assertTrue(createdScan.getFilter() instanceof ExcludeDataColumnFilter);
  }

  @Test
  public void testAddLockAndWriteColumnToGet() throws IOException {
    byte[][] families = new byte[][] { FAMILY, FAMILY, ANOTHER_FAMILY };
    byte[][] qualifiers = new byte[][] { QUALIFIER, ANOTHER_QUALIFIER, QUALIFIER };
    Get userGet = new Get(ROW);
    for (int i = 0; i < families.length; ++i) {
      userGet.addColumn(families[i], qualifiers[i]);
    }
    Get internalGet = new Get(userGet.getRow());
    ThemisCpUtil.addLockAndWriteColumnToGet(userGet, internalGet, PREWRITE_TS);
    assertEquals(3, internalGet.getFamilyMap().size());
    if (ColumnUtil.isCommitToSameFamily()) {
      assertEquals(4, internalGet.getFamilyMap().get(FAMILY).size());
      assertEquals(2, internalGet.getFamilyMap().get(ANOTHER_FAMILY).size());
    } else {
      assertEquals(3, internalGet.getFamilyMap().get(ColumnUtil.PUT_FAMILY_NAME_BYTES).size());
      assertEquals(3, internalGet.getFamilyMap().get(ColumnUtil.DELETE_FAMILY_NAME_BYTES).size());
    }
    assertEquals(3, internalGet.getFamilyMap().get(ColumnUtil.LOCK_FAMILY_NAME).size());
    for (int i = 0; i < families.length; ++i) {
      checkReadWithLockAndWriteColumns(internalGet.getFamilyMap(),
        new ColumnCoordinate(families[i], qualifiers[i]));
    }
    assertEquals(PREWRITE_TS, internalGet.getTimeRange().getMax());
    assertEquals(1, internalGet.getMaxVersions());

    // test for family-level transfer
    userGet = new Get(ROW);
    userGet.addFamily(FAMILY);
    internalGet = new Get(userGet.getRow());
    ThemisCpUtil.addLockAndWriteColumnToGet(userGet, internalGet, PREWRITE_TS);
    if (ColumnUtil.isCommitToSameFamily()) {
      checkAddLockAndDataFamily(internalGet, ColumnUtil.LOCK_FAMILY_NAME, FAMILY);
    } else {
      checkAddLockAndDataFamily(internalGet, ColumnUtil.LOCK_FAMILY_NAME,
        ColumnUtil.PUT_FAMILY_NAME_BYTES, ColumnUtil.DELETE_FAMILY_NAME_BYTES);
    }
    assertTrue(internalGet.getFilter() instanceof ExcludeDataColumnFilter);

    // test for combination of family-level and column-level transfer
    userGet = new Get(ROW);
    userGet.addFamily(ANOTHER_FAMILY); // another family will be processed firstly
    userGet.addColumn(FAMILY, ANOTHER_QUALIFIER); // the column should not overwrite lock family
    internalGet = new Get(userGet.getRow());
    ThemisCpUtil.addLockAndWriteColumnToGet(userGet, internalGet, PREWRITE_TS);
    if (ColumnUtil.isCommitToSameFamily()) {
      checkAddLockAndDataFamily(internalGet, ColumnUtil.LOCK_FAMILY_NAME, ANOTHER_FAMILY);
      checkReadWithWriteColumns(internalGet.getFamilyMap(),
        new ColumnCoordinate(FAMILY, ANOTHER_QUALIFIER));
    } else {
      checkAddLockAndDataFamily(internalGet, ColumnUtil.LOCK_FAMILY_NAME,
        ColumnUtil.PUT_FAMILY_NAME_BYTES, ColumnUtil.DELETE_FAMILY_NAME_BYTES);
    }
    assertTrue(internalGet.getFilter() instanceof ExcludeDataColumnFilter);
  }

  @Test
  public void testRemoveNotRequiredLockColumns() {
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    get.addColumn(ANOTHER_FAMILY, ANOTHER_QUALIFIER);
    List<Cell> sourceKvs = new ArrayList<>();
    sourceKvs.add(KEYVALUE);
    Column lockColumn = ColumnUtil.getLockColumn(FAMILY, QUALIFIER);
    Cell lockKv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW)
      .setFamily(lockColumn.getFamily()).setQualifier(lockColumn.getQualifier())
      .setTimestamp(PREWRITE_TS).setType(Type.Put).setValue(VALUE).build();
    sourceKvs.add(lockKv);
    lockColumn = ColumnUtil.getLockColumn(ANOTHER_FAMILY, QUALIFIER);
    sourceKvs.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW)
      .setFamily(lockColumn.getFamily()).setQualifier(lockColumn.getQualifier())
      .setTimestamp(PREWRITE_TS).setType(Type.Put).setValue(VALUE).build());
    Result result =
      ThemisCpUtil.removeNotRequiredLockColumns(get.getFamilyMap(), Result.create(sourceKvs));
    assertEquals(2, result.size());
    assertTrue(KEYVALUE.equals(result.rawCells()[0]));
    assertTrue(lockKv.equals(result.rawCells()[1]));
  }

  protected static void checkAddLockAndDataFamily(Get internalGet, byte[]... families) {
    for (byte[] family : families) {
      assertTrue(internalGet.getFamilyMap().containsKey(family));
      assertNull(internalGet.getFamilyMap().get(family));
    }
    assertTrue(internalGet.getFilter() instanceof ExcludeDataColumnFilter);

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
    assertTrue(families.get(lockColumn.getFamily()).contains(lockColumn.getQualifier()));
    checkReadWithWriteColumns(families, columnCoordinate);
  }

  public static void checkReadWithWriteColumns(Map<byte[], NavigableSet<byte[]>> families,
      ColumnCoordinate columnCoordinate) {
    Column putColumn = ColumnUtil.getPutColumn(columnCoordinate);
    assertTrue(families.get(putColumn.getFamily()).contains(putColumn.getQualifier()));
    Column delColumn = ColumnUtil.getDeleteColumn(columnCoordinate);
    assertTrue(families.get(delColumn.getFamily()).contains(delColumn.getQualifier()));
  }

  @Test
  public void testAddLockAndWriteColumnToScan() {
    Scan scan = new Scan();
    ThemisCpUtil.addLockAndWriteColumnToScan(COLUMN, scan);
    checkReadWithLockAndWriteColumns(scan.getFamilyMap(), COLUMN);

    // scan with family
    scan = new Scan();
    scan.addFamily(ColumnUtil.LOCK_FAMILY_NAME);
    ThemisCpUtil.addLockAndWriteColumnToScan(COLUMN, scan);
    assertTrue(scan.getFamilyMap().containsKey(ColumnUtil.LOCK_FAMILY_NAME) &&
      scan.getFamilyMap().get(ColumnUtil.LOCK_FAMILY_NAME) == null);
    checkReadWithWriteColumns(scan.getFamilyMap(), COLUMN);
  }

  @Test
  public void testIsLockResult() {
    List<Cell> kvs = new ArrayList<>();
    kvs.add(KEYVALUE);
    assertFalse(ThemisCpUtil.isLockResult(Result.create(kvs)));
    kvs.add(getLockKv(KEYVALUE));
    assertFalse(ThemisCpUtil.isLockResult(Result.create(kvs)));
    kvs.clear();
    kvs.add(getLockKv(KEYVALUE));
    assertTrue(ThemisCpUtil.isLockResult(Result.create(kvs)));
    kvs.add(KEYVALUE);
    assertTrue(ThemisCpUtil.isLockResult(Result.create(kvs)));
  }

  @Test
  public void testSeperateLockAndWriteKvs() {
    List<Cell> kvs = new ArrayList<>();
    kvs.add(KEYVALUE);
    kvs.add(getLockKv(KEYVALUE));
    kvs.add(getPutKv(KEYVALUE));
    Column column = ColumnUtil.getDeleteColumn(new Column(FAMILY, QUALIFIER));
    Cell deleteKv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW)
      .setFamily(column.getFamily()).setQualifier(column.getQualifier()).setTimestamp(PREWRITE_TS)
      .setType(Type.Put).setValue(VALUE).build();
    kvs.add(deleteKv);
    Pair<List<Cell>, List<Cell>> result =
      ThemisCpUtil.seperateLockAndWriteKvs(kvs.toArray(new Cell[0]));
    assertEquals(1, result.getFirst().size());
    assertCellEquals(getLockKv(KEYVALUE), result.getFirst().get(0));
    assertEquals(2, result.getSecond().size());
    assertCellEquals(getPutKv(KEYVALUE), result.getSecond().get(0));
    assertCellEquals(deleteKv, result.getSecond().get(1));

    result = ThemisCpUtil.seperateLockAndWriteKvs(null);
    assertEquals(0, result.getFirst().size());
    assertEquals(0, result.getSecond().size());
  }
}
