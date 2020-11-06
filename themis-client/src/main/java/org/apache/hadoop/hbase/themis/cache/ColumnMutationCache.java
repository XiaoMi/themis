package org.apache.hadoop.hbase.themis.cache;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.themis.ConcurrentRowCallables.TableAndRow;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

// local cache themis transaction
public class ColumnMutationCache {
  // index mutations by table and row
  private Map<byte[], Map<byte[], RowMutation>> mutations =
      new TreeMap<>(Bytes.BYTES_COMPARATOR);
  
  public boolean addMutation(byte[] tableName, Cell kv) {
    Map<byte[], RowMutation> rowMutations = mutations.computeIfAbsent(tableName,
            k -> new TreeMap<>(Bytes.BYTES_COMPARATOR));


    final byte[] rowKey = CellUtil.cloneRow(kv);
    RowMutation rowMutation = rowMutations.get(rowKey);
    if (rowMutation == null) {
      rowMutation = new RowMutation(rowKey);
      rowMutations.put(rowKey, rowMutation);
    }
    return rowMutation.addMutation(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv), kv.getType(), CellUtil.cloneValue(kv));
  }
  
  public Set<Entry<byte[], Map<byte[], RowMutation>>> getMutations() {
    return mutations.entrySet();
  }
  
  // return <rowCount, columnCount>
  public Pair<Integer, Integer> getMutationsCount() {
    int rowCount = 0;
    int columnCount = 0;
    for (Entry<byte[], Map<byte[], RowMutation>> tableEntry : mutations.entrySet()) {
      for (Entry<byte[], RowMutation> rowEntry : tableEntry.getValue().entrySet()) {
        ++rowCount;
        columnCount += rowEntry.getValue().size();
      }
    }
    return new Pair<Integer, Integer>(rowCount, columnCount);
  }
  
  public int size() {
    int size = 0;
    for (Entry<byte[], Map<byte[], RowMutation>> tableEntry : mutations.entrySet()) {
      for (Entry<byte[], RowMutation> rowEntry : tableEntry.getValue().entrySet()) {
        size += rowEntry.getValue().size();
      }
    }
    return size;
  }
  
  public boolean hasMutation(ColumnCoordinate columnCoordinate) {
    return getMutation(columnCoordinate) != null;
  }
  
  public Pair<Cell.Type, byte[]> getMutation(ColumnCoordinate column) {
    Map<byte[], RowMutation> tableMutation = mutations.get(column.getTableName());
    if (tableMutation != null) {
      RowMutation rowMutation = tableMutation.get(column.getRow());
      if (rowMutation != null) {
        return rowMutation.getMutation(column);
      }
    }
    return null;
  }
  
  public Cell.Type getType(ColumnCoordinate columnCoordinate) {
    Pair<Cell.Type, byte[]> mutation = getMutation(columnCoordinate);
    return mutation == null ? null : mutation.getFirst();
  }
  
  public RowMutation getRowMutation(TableAndRow tableAndRow) {
    return getRowMutation(tableAndRow.getTableName(), tableAndRow.getRowkey());
  }
  
  public RowMutation getRowMutation(byte[] tableName, byte[] rowkey) {
    Map<byte[], RowMutation> rowMutations = mutations.get(tableName);
    return rowMutations == null ? null : rowMutations.get(rowkey);
  }
}