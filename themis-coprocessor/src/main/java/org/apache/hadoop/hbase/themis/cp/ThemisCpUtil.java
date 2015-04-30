package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
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
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._;

public class ThemisCpUtil {
  // Filters which only use the rowkey will be classified into ALLOWED_ROWKEY_FILTER_CLASSES class,
  // these filters
  // will be used in the first stage of themis read
  public static Set<Class<? extends Filter>> ALLOWED_ROWKEY_FILTER_CLASSES = new HashSet<Class<? extends Filter>>();
  public static Set<Class<? extends Filter>> DISALLOWD_FILTERS = new HashSet<Class<? extends Filter>>();
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
          if (!(internalGet.getFamilyMap().containsKey(ColumnUtil.LOCK_FAMILY_NAME) && internalGet
              .getFamilyMap().get(ColumnUtil.LOCK_FAMILY_NAME) == null)) {
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
    if (!result.isEmpty()) {
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      for (KeyValue kv : result.list()) {
        if (Bytes.equals(ColumnUtil.LOCK_FAMILY_NAME, kv.getFamily())
            || ColumnUtil.isCommitFamily(kv.getFamily())) {
          Column dataColumn = ColumnUtil.getDataColumnFromConstructedQualifier(new Column(kv.getFamily(), kv
              .getQualifier()));
          if (familyMap.containsKey(dataColumn.getFamily())) {
            Set<byte[]> qualifiers= familyMap.get(dataColumn.getFamily());
            // for scan, after serialization, the null qualifiers will be set to empty set
            if (qualifiers == null || qualifiers.size() == 0
                || qualifiers.contains(dataColumn.getQualifier())) {
              kvs.add(kv);
            }
          }
        } else {
          kvs.add(kv);
        }
      }
      if (kvs.size() != result.size()) {
        return new Result(kvs);
      }
    }
    return result;
  }

  public static Get constructLockAndWriteGet(Get userGet, long startTs) throws IOException {
    Get putGet = new Get(userGet.getRow());
    addLockAndWriteColumnToGet(userGet, putGet, startTs);
    moveRowkeyFiltersForWriteGet(userGet, putGet);
    return putGet;
  }

  public static void prepareGet(Get get, Collection<HColumnDescriptor> families) {
    if (!get.hasFamilies()) {
      for (HColumnDescriptor family : families) {
        if (!Bytes.equals(family.getName(), ColumnUtil.LOCK_FAMILY_NAME)) {
          if (ColumnUtil.isCommitToSameFamily() || !ColumnUtil.isCommitFamily(family.getName())) {
            get.addFamily(family.getName());
          }
        }
      }
    }
  }

  public static void prepareScan(Scan scan, Collection<HColumnDescriptor> families) {
    if (!scan.hasFamilies()) {
      for (HColumnDescriptor family : families) {
        if (!Bytes.equals(family.getName(), ColumnUtil.LOCK_FAMILY_NAME)) {
          if (ColumnUtil.isCommitToSameFamily() || !ColumnUtil.isCommitFamily(family.getName())) {
            scan.addFamily(family.getName());
          }
        }
      }
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
    if (!(scan.getFamilyMap().containsKey(ColumnUtil.LOCK_FAMILY_NAME) && scan
        .getFamilyMap().get(ColumnUtil.LOCK_FAMILY_NAME) == null)) {
      Column lockColumn = ColumnUtil.getLockColumn(column);
      scan.addColumn(lockColumn.getFamily(), lockColumn.getQualifier());
    }
    Column putColumn = ColumnUtil.getPutColumn(column);
    if (ColumnUtil.isCommitToSameFamily()
        || !(scan.getFamilyMap().containsKey(ColumnUtil.PUT_FAMILY_NAME_BYTES) && scan
            .getFamilyMap().get(ColumnUtil.PUT_FAMILY_NAME_BYTES) == null)) {
      scan.addColumn(putColumn.getFamily(), putColumn.getQualifier());
    }
    Column deleteColumn = ColumnUtil.getDeleteColumn(column);
    if (ColumnUtil.isCommitToSameFamily()
        || !(scan.getFamilyMap().containsKey(ColumnUtil.DELETE_FAMILY_NAME_BYTES) && scan
            .getFamilyMap().get(ColumnUtil.DELETE_FAMILY_NAME_BYTES) == null)) {
      scan.addColumn(deleteColumn.getFamily(), deleteColumn.getQualifier());
    }
  }

  public static Get constructDataGetByPutKvs(List<KeyValue> putKvs, Filter filter)
      throws IOException {
    Get get = new Get(putKvs.get(0).getRow());
    long minTs = Long.MAX_VALUE;
    long maxTs = 0;
    ColumnTimestampFilter timestampFilter = new ColumnTimestampFilter();
    for (int i = 0; i < putKvs.size(); ++i) {
      KeyValue putKv = putKvs.get(i);
      Column putColumn = new Column(putKv.getFamily(), putKv.getQualifier());
      Column dataColumn = ColumnUtil.getDataColumn(putColumn);
      get.addColumn(dataColumn.getFamily(), dataColumn.getQualifier());
      long prewriteTs = Bytes.toLong(putKv.getValue());
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
  protected static List<KeyValue> getPutKvs(List<KeyValue> writeKvs) {
    if (ColumnUtil.isCommitToSameFamily()) {
      List<KeyValue> result = new ArrayList<KeyValue>();
      Column lastDataColumn = null;
      KeyValue lastKv = null;
      for (int i = 0; i < writeKvs.size(); ++i) {
        KeyValue kv = writeKvs.get(i);
        Column column = new Column(kv.getFamily(), kv.getQualifier());
        Column dataColumn = ColumnUtil.getDataColumn(column);
        if (lastDataColumn != null && lastDataColumn.equals(dataColumn)) {
          if (lastKv.getTimestamp() < kv.getTimestamp()) {
            lastKv = kv;
          }
        } else {
          if (lastKv != null && ColumnUtil.isPutColumn(lastKv.getFamily(), lastKv.getQualifier())) {
            result.add(lastKv);
          }
          lastDataColumn = dataColumn;
          lastKv = kv;
        }
      }
      if (lastKv != null && ColumnUtil.isPutColumn(lastKv.getFamily(), lastKv.getQualifier())) {
        result.add(lastKv);
      }
      return result;
    } else {
      return getPutKvsForCommitDifferentFamily(writeKvs);
    }
  }
  
  protected static List<KeyValue> getPutKvsForCommitDifferentFamily(List<KeyValue> writeKvs) {
    Map<Column, KeyValue> map = new HashMap<Column, KeyValue>();
    for (KeyValue kv : writeKvs) {
      Column column = new Column(kv.getFamily(), kv.getQualifier());
      Column dataColumn = ColumnUtil.getDataColumn(column);
      KeyValue existKv = map.get(dataColumn);
      if (existKv == null || kv.getTimestamp() > existKv.getTimestamp()) {
        map.put(dataColumn, kv);
      }
    }
    List<KeyValue> result = new ArrayList<KeyValue>();
    for (Entry<Column, KeyValue> entry : map.entrySet()) {
      KeyValue kv = entry.getValue();
      if (ColumnUtil.isPutColumn(kv.getFamily(), kv.getQualifier())) {
        result.add(kv);
      }
    }
    return result;
  }

  public static void addWriteColumnToGet(Column column, Get get) {
    Column putColumn = ColumnUtil.getPutColumn(column);
    if (ColumnUtil.isCommitToSameFamily()
        || !(get.getFamilyMap().containsKey(ColumnUtil.PUT_FAMILY_NAME_BYTES) && get.getFamilyMap()
            .get(ColumnUtil.PUT_FAMILY_NAME_BYTES) == null)) {
      get.addColumn(putColumn.getFamily(), putColumn.getQualifier());
    }
    Column deleteColumn = ColumnUtil.getDeleteColumn(column);
    if (ColumnUtil.isCommitToSameFamily()
        || !(get.getFamilyMap().containsKey(ColumnUtil.DELETE_FAMILY_NAME_BYTES) && get
            .getFamilyMap()
        .get(ColumnUtil.DELETE_FAMILY_NAME_BYTES) == null)) {
      get.addColumn(deleteColumn.getFamily(), deleteColumn.getQualifier());
    }
  }

  public static boolean isLockResult(Result result) {
    if (result.isEmpty()) {
      return false;
    }
    KeyValue firstKv = result.list().get(0);
    return ColumnUtil.isLockColumn(firstKv.getFamily(), firstKv.getQualifier());
  }

  public static Pair<List<KeyValue>, List<KeyValue>> seperateLockAndWriteKvs(List<KeyValue> kvs) {
    List<KeyValue> lockKvs = new ArrayList<KeyValue>();
    List<KeyValue> writeKvs = new ArrayList<KeyValue>();
    if (kvs != null) {
      for (KeyValue kv : kvs) {
        if (ColumnUtil.isLockColumn(kv.getFamily(), kv.getQualifier())) {
          lockKvs.add(kv);
        } else if (ColumnUtil.isWriteColumn(kv.getFamily(), kv.getQualifier())) {
          writeKvs.add(kv);
        }
      }
    }
    return new Pair<List<KeyValue>, List<KeyValue>>(lockKvs, writeKvs);
  }  
}