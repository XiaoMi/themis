package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class ThemisCpUtil {
  // Filters which only use the rowkey will be classified into ALLOWED_ROWKEY_FILTER_CLASSES class,
  // these filters
  // will be used in the first stage of themis read
  public static Set<Class<? extends Filter>> ALLOWED_ROWKEY_FILTER_CLASSES =
    new HashSet<Class<? extends Filter>>();
  public static Set<Class<? extends Filter>> DISALLOWD_FILTERS =
    new HashSet<Class<? extends Filter>>();
  private static String disallowedFilterClassNameString = null;

  static {
    ALLOWED_ROWKEY_FILTER_CLASSES.add(InclusiveStopFilter.class);
    ALLOWED_ROWKEY_FILTER_CLASSES.add(PrefixFilter.class);
    ALLOWED_ROWKEY_FILTER_CLASSES.add(RandomRowFilter.class);
    ALLOWED_ROWKEY_FILTER_CLASSES.add(FuzzyRowFilter.class);
    ALLOWED_ROWKEY_FILTER_CLASSES.add(InclusiveStopFilter.class);
    ALLOWED_ROWKEY_FILTER_CLASSES.add(PageFilter.class);
    ALLOWED_ROWKEY_FILTER_CLASSES.add(RowFilter.class);

    DISALLOWD_FILTERS.add(DependentColumnFilter.class); // need timestamp
    // TODO : check the wrapped class to judge whether allowed
    DISALLOWD_FILTERS.add(SkipFilter.class);
    DISALLOWD_FILTERS.add(WhileMatchFilter.class);
  }

  public static String getDisallowedFilterClassNameString() {
    if (disallowedFilterClassNameString == null) {
      disallowedFilterClassNameString = "";
      for (Class<? extends Filter> cls : DISALLOWD_FILTERS) {
        disallowedFilterClassNameString += (cls.getName() + ";");
      }
    }
    return disallowedFilterClassNameString;
  }

  public static interface RowLevelFilter {
    public boolean isRowLevelFilter();
  }

  public static abstract class FilterCallable {
    public abstract void processConcreteFilter(Filter filter) throws IOException;

    public boolean processFilterListOperator(Operator op) {
      return true;
    }
  }

  public static void processFilters(Filter filter, FilterCallable callable) throws IOException {
    if (filter != null) {
      if (filter instanceof FilterList) {
        FilterList filterList = (FilterList) filter;
        if (!callable.processFilterListOperator(filterList.getOperator())) {
          return;
        }
        for (Filter filterInList : filterList.getFilters()) {
          processFilters(filterInList, callable);
        }
      } else {
        callable.processConcreteFilter(filter);
      }
    }
  }

  static class ClassifiedFilters extends FilterCallable {
    protected boolean allMustPassOperator = true;
    protected FilterList rowkeyFilters = new FilterList();
    protected FilterList noRowkeyFilters = new FilterList();

    @Override
    public void processConcreteFilter(Filter filter) throws IOException {
      if (ALLOWED_ROWKEY_FILTER_CLASSES.contains(filter.getClass())) {
        rowkeyFilters.addFilter(filter);
      } else if (filter instanceof RowLevelFilter) {
        if (((RowLevelFilter) filter).isRowLevelFilter()) {
          rowkeyFilters.addFilter(filter);
        } else {
          noRowkeyFilters.addFilter(filter);
        }
      } else {
        noRowkeyFilters.addFilter(filter);
      }
    }

    public boolean processFilterListOperator(Operator op) {
      if (op.equals(Operator.MUST_PASS_ONE)) {
        allMustPassOperator = false;
        return false;
      }
      return true;
    }
  }

  // we will first read lock/write column when doing themis read to check lock conflict.
  // at this time, we could move ROWKEY_FILTERS to the first read if all filters in user-set
  // filterList are organized by MUST_PASS_ALL, which could filter data as early as possible
  public static void moveRowkeyFiltersForWriteGet(Get sourceGet, Get destGet) throws IOException {
    ClassifiedFilters classifyFilters = new ClassifiedFilters();
    if (sourceGet.getFilter() != null) {
      processFilters(sourceGet.getFilter(), classifyFilters);
    }
    if (classifyFilters.allMustPassOperator) {
      if (classifyFilters.rowkeyFilters.getFilters().size() != 0) {
        if (destGet.getFilter() != null) {
          FilterList filter = new FilterList();
          filter.addFilter(destGet.getFilter());
          filter.addFilter(classifyFilters.rowkeyFilters);
          destGet.setFilter(filter);
        } else {
          destGet.setFilter(classifyFilters.rowkeyFilters);
        }
        sourceGet.setFilter(classifyFilters.noRowkeyFilters);
      }
    }
  }

  public static void moveRowkeyFiltersForWriteScan(Scan sourceScan, Scan destScan)
      throws IOException {
    ClassifiedFilters classifyFilters = new ClassifiedFilters();
    if (sourceScan.getFilter() != null) {
      processFilters(sourceScan.getFilter(), classifyFilters);
    }
    if (classifyFilters.allMustPassOperator) {
      if (classifyFilters.rowkeyFilters.getFilters().size() != 0) {
        if (destScan.getFilter() != null) {
          FilterList filter = new FilterList();
          filter.addFilter(destScan.getFilter());
          filter.addFilter(classifyFilters.rowkeyFilters);
          destScan.setFilter(filter);
        } else {
          destScan.setFilter(classifyFilters.rowkeyFilters);
        }
        sourceScan.setFilter(classifyFilters.noRowkeyFilters);
      }
    }
  }

  public static void addLockAndWriteColumnToGet(Get userGet, Get internalGet, long startTs)
      throws IOException {
    boolean excludeDataColumn = false;
    for (Entry<byte[], NavigableSet<byte[]>> entry : userGet.getFamilyMap().entrySet()) {
      if (entry.getValue() != null && entry.getValue().size() > 0) {
        for (byte[] qualifier : entry.getValue()) {
          Column dataColumn = new Column(entry.getKey(), qualifier);
          // not include the whole lock family
          if (!(internalGet.getFamilyMap().containsKey(ColumnUtil.LOCK_FAMILY_NAME) &&
            internalGet.getFamilyMap().get(ColumnUtil.LOCK_FAMILY_NAME) == null)) {
            Column lockColumn = ColumnUtil.getLockColumn(dataColumn);
            internalGet.addColumn(lockColumn.getFamily(), lockColumn.getQualifier());
          }
          addWriteColumnToGet(dataColumn, internalGet);
        }
      } else {
        // TODO : use filter to read out lock columns corresponding to needed data column
        internalGet.addFamily(ColumnUtil.LOCK_FAMILY_NAME);
        if (ColumnUtil.isCommitToSameFamily()) {
          internalGet.addFamily(entry.getKey());
        } else {
          internalGet.addFamily(ColumnUtil.PUT_FAMILY_NAME_BYTES);
          internalGet.addFamily(ColumnUtil.DELETE_FAMILY_NAME_BYTES);
        }
        excludeDataColumn = true;
      }
    }
    if (excludeDataColumn) {
      internalGet.setFilter(new ExcludeDataColumnFilter());
    }
    internalGet.setTimeRange(0, startTs);
  }

  public static Result removeNotRequiredLockColumns(Map<byte[], NavigableSet<byte[]>> familyMap,
      Result result) {
    if (result.isEmpty()) {
      return result;
    }
    List<Cell> cells = new ArrayList<>();

    for (Cell cell : result.rawCells()) {
      byte[] family = CellUtil.cloneFamily(cell);
      if (Bytes.equals(ColumnUtil.LOCK_FAMILY_NAME, family) || ColumnUtil.isCommitFamily(family)) {
        Column dataColumn = ColumnUtil
          .getDataColumnFromConstructedQualifier(new Column(family, CellUtil.cloneQualifier(cell)));
        if (familyMap.containsKey(dataColumn.getFamily())) {
          Set<byte[]> qualifiers = familyMap.get(dataColumn.getFamily());
          // for scan, after serialization, the null qualifiers will be set to empty set
          if (qualifiers == null || qualifiers.size() == 0 ||
            qualifiers.contains(dataColumn.getQualifier())) {
            cells.add(cell);
          }
        }
      } else {
        cells.add(cell);
      }
    }
    return cells.size() != result.size() ? Result.create(cells) : result;
  }

  public static Get constructLockAndWriteGet(Get userGet, long startTs) throws IOException {
    Get putGet = new Get(userGet.getRow());
    addLockAndWriteColumnToGet(userGet, putGet, startTs);
    moveRowkeyFiltersForWriteGet(userGet, putGet);
    return putGet;
  }

  private static void addFamilies(ColumnFamilyDescriptor[] families, Consumer<byte[]> action) {
    Stream.of(families).map(ColumnFamilyDescriptor::getName)
      .filter(fn -> !Bytes.equals(fn, ColumnUtil.LOCK_FAMILY_NAME))
      .filter(fn -> ColumnUtil.isCommitToSameFamily() || !ColumnUtil.isCommitFamily(fn))
      .forEach(action);
  }

  public static void prepareGet(Get get, ColumnFamilyDescriptor[] families) {
    if (get.hasFamilies()) {
      return;
    }
    addFamilies(families, get::addFamily);
  }

  public static void prepareScan(Scan scan, ColumnFamilyDescriptor[] families) {
    if (!scan.hasFamilies()) {
      addFamilies(families, scan::addFamily);
    } else {
      // before ThemisScanObserver.preScannerOpen is invoked, all families of the table will
      // be added the the scan if scan the whole row, so that we need remove lock family
      scan.getFamilyMap().remove(ColumnUtil.LOCK_FAMILY_NAME);
      if (!ColumnUtil.isCommitToSameFamily()) {
        scan.getFamilyMap().remove(ColumnUtil.PUT_FAMILY_NAME_BYTES);
        scan.getFamilyMap().remove(ColumnUtil.DELETE_FAMILY_NAME_BYTES);
      }
    }
  }

  public static Scan constructLockAndWriteScan(Scan userScan, long startTs) throws IOException {
    boolean excludeDataColumn = false;
    Scan internalScan = new Scan(userScan);
    internalScan.setFilter(null);
    internalScan.setFamilyMap(new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR));
    for (Entry<byte[], NavigableSet<byte[]>> entry : userScan.getFamilyMap().entrySet()) {
      if (entry.getValue() != null && entry.getValue().size() > 0) {
        for (byte[] qualifier : entry.getValue()) {
          Column dataColumn = new Column(entry.getKey(), qualifier);
          addLockAndWriteColumnToScan(dataColumn, internalScan);
        }
      } else {
        // TODO : use filter to read out lock columns corresponding to needed data column
        internalScan.addFamily(ColumnUtil.LOCK_FAMILY_NAME);
        internalScan.addFamily(entry.getKey());
        if (!ColumnUtil.isCommitToSameFamily()) {
          internalScan.addFamily(ColumnUtil.PUT_FAMILY_NAME_BYTES);
          internalScan.addFamily(ColumnUtil.DELETE_FAMILY_NAME_BYTES);
        }
        excludeDataColumn = true;
      }
    }
    if (excludeDataColumn) {
      internalScan.setFilter(new ExcludeDataColumnFilter());
    }
    internalScan.setTimeRange(0, startTs);
    moveRowkeyFiltersForWriteScan(userScan, internalScan);
    return internalScan;
  }

  public static void addLockAndWriteColumnToScan(Column column, Scan scan) {
    // avoid overwrite the whole lock family
    if (!(scan.getFamilyMap().containsKey(ColumnUtil.LOCK_FAMILY_NAME) &&
      scan.getFamilyMap().get(ColumnUtil.LOCK_FAMILY_NAME) == null)) {
      Column lockColumn = ColumnUtil.getLockColumn(column);
      scan.addColumn(lockColumn.getFamily(), lockColumn.getQualifier());
    }
    Column putColumn = ColumnUtil.getPutColumn(column);
    if (ColumnUtil.isCommitToSameFamily() ||
      !(scan.getFamilyMap().containsKey(ColumnUtil.PUT_FAMILY_NAME_BYTES) &&
        scan.getFamilyMap().get(ColumnUtil.PUT_FAMILY_NAME_BYTES) == null)) {
      scan.addColumn(putColumn.getFamily(), putColumn.getQualifier());
    }
    Column deleteColumn = ColumnUtil.getDeleteColumn(column);
    if (ColumnUtil.isCommitToSameFamily() ||
      !(scan.getFamilyMap().containsKey(ColumnUtil.DELETE_FAMILY_NAME_BYTES) &&
        scan.getFamilyMap().get(ColumnUtil.DELETE_FAMILY_NAME_BYTES) == null)) {
      scan.addColumn(deleteColumn.getFamily(), deleteColumn.getQualifier());
    }
  }

  public static Get constructDataGetByPutKvs(List<Cell> putKvs, Filter filter) throws IOException {
    Cell firstCell = putKvs.get(0);
    Get get = new Get(firstCell.getRowArray(), firstCell.getRowOffset(), firstCell.getRowLength());
    long minTs = Long.MAX_VALUE;
    long maxTs = 0;
    ColumnTimestampFilter timestampFilter = new ColumnTimestampFilter();
    for (Cell putKv : putKvs) {
      Column putColumn = new Column(CellUtil.cloneFamily(putKv), CellUtil.cloneQualifier(putKv));
      Column dataColumn = ColumnUtil.getDataColumn(putColumn);
      get.addColumn(dataColumn.getFamily(), dataColumn.getQualifier());
      long prewriteTs =
        Bytes.toLong(putKv.getValueArray(), putKv.getValueOffset(), putKv.getValueLength());
      timestampFilter.addColumnTimestamp(dataColumn, prewriteTs);
      if (minTs > prewriteTs) {
        minTs = prewriteTs;
      }
      if (maxTs < prewriteTs) {
        maxTs = prewriteTs;
      }
    }
    // minTs and maxTs should be updated to contain all the prewriteTs which needs to be read
    get.setTimeRange(minTs, maxTs + 1);
    if (filter == null) {
      get.setFilter(timestampFilter);
    } else {
      FilterList filterList = new FilterList();
      filterList.addFilter(timestampFilter);
      filterList.addFilter(filter);
      get.setFilter(filterList);
    }
    return get;
  }

  // get put kv which has greater timestamp than delete kv under the same family
  public static List<Cell> getPutKvs(List<Cell> writeKvs) {
    if (ColumnUtil.isCommitToSameFamily()) {
      List<Cell> result = new ArrayList<>();
      Column lastDataColumn = null;
      Cell lastKv = null;
      for (int i = 0; i < writeKvs.size(); ++i) {
        Cell kv = writeKvs.get(i);
        Column column = new Column(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv));
        Column dataColumn = ColumnUtil.getDataColumn(column);
        if (lastDataColumn != null && lastDataColumn.equals(dataColumn)) {
          if (lastKv.getTimestamp() < kv.getTimestamp()) {
            lastKv = kv;
          }
        } else {
          if (lastKv != null &&
            ColumnUtil.isPutColumn(CellUtil.cloneFamily(lastKv), CellUtil.cloneQualifier(lastKv))) {
            result.add(lastKv);
          }
          lastDataColumn = dataColumn;
          lastKv = kv;
        }
      }
      if (lastKv != null &&
        ColumnUtil.isPutColumn(CellUtil.cloneFamily(lastKv), CellUtil.cloneQualifier(lastKv))) {
        result.add(lastKv);
      }
      return result;
    } else {
      return getPutKvsForCommitDifferentFamily(writeKvs);
    }
  }

  private static List<Cell> getPutKvsForCommitDifferentFamily(List<Cell> writeKvs) {
    Map<Column, Cell> map = new LinkedHashMap<>();
    for (Cell kv : writeKvs) {
      Column column = new Column(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv));
      Column dataColumn = ColumnUtil.getDataColumn(column);
      Cell existKv = map.get(dataColumn);
      if (existKv == null || kv.getTimestamp() > existKv.getTimestamp()) {
        map.put(dataColumn, kv);
      }
    }
    List<Cell> result = new ArrayList<>();
    map.forEach((column, cell) -> {
      if (ColumnUtil.isPutColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell))) {
        result.add(cell);
      }
    });
    return result;
  }

  public static void addWriteColumnToGet(Column column, Get get) {
    Column putColumn = ColumnUtil.getPutColumn(column);
    if (ColumnUtil.isCommitToSameFamily() ||
      !(get.getFamilyMap().containsKey(ColumnUtil.PUT_FAMILY_NAME_BYTES) &&
        get.getFamilyMap().get(ColumnUtil.PUT_FAMILY_NAME_BYTES) == null)) {
      get.addColumn(putColumn.getFamily(), putColumn.getQualifier());
    }
    Column deleteColumn = ColumnUtil.getDeleteColumn(column);
    if (ColumnUtil.isCommitToSameFamily() ||
      !(get.getFamilyMap().containsKey(ColumnUtil.DELETE_FAMILY_NAME_BYTES) &&
        get.getFamilyMap().get(ColumnUtil.DELETE_FAMILY_NAME_BYTES) == null)) {
      get.addColumn(deleteColumn.getFamily(), deleteColumn.getQualifier());
    }
  }

  public static boolean isLockResult(Result result) {
    if (result.isEmpty()) {
      return false;
    }
    Cell firstCell = result.rawCells()[0];
    return ColumnUtil.isLockColumn(CellUtil.cloneFamily(firstCell),
      CellUtil.cloneQualifier(firstCell));
  }

  public static Pair<List<Cell>, List<Cell>> seperateLockAndWriteKvs(Cell[] kvs) {
    List<Cell> lockKvs = new ArrayList<>();
    List<Cell> writeKvs = new ArrayList<>();
    if (kvs != null) {
      for (Cell kv : kvs) {
        byte[] family = CellUtil.cloneFamily(kv);
        byte[] qualifier = CellUtil.cloneQualifier(kv);
        if (ColumnUtil.isLockColumn(family, qualifier)) {
          lockKvs.add(kv);
        } else if (ColumnUtil.isWriteColumn(family, qualifier)) {
          writeKvs.add(kv);
        }
      }
    }
    return new Pair<>(lockKvs, writeKvs);
  }
}