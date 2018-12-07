package org.apache.hadoop.hbase.themis.cache;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.themis.ConcurrentRowCallables.TableAndRow;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

// local cache themis transaction
public class ColumnMutationCache {
  // index mutations by table and row
  private Map<TableName, Map<byte[], RowMutation>> mutations = new TreeMap<>();

  public boolean addMutation(TableName tableName, Cell kv) {
    Map<byte[], RowMutation> rowMutations = mutations.get(tableName);
    if (rowMutations == null) {
      rowMutations = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      mutations.put(tableName, rowMutations);
    }
    byte[] row = CellUtil.cloneRow(kv);
    RowMutation rowMutation = rowMutations.get(row);
    if (rowMutation == null) {
      rowMutation = new RowMutation(row);
      rowMutations.put(row, rowMutation);
    }
    return rowMutation.addMutation(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv),
      kv.getType(), CellUtil.cloneValue(kv));
  }

  public Set<Entry<TableName, Map<byte[], RowMutation>>> getMutations() {
    return mutations.entrySet();
  }

  // return <rowCount, columnCount>
  public Pair<Integer, Integer> getMutationsCount() {
    int rowCount = 0;
    int columnCount = 0;
    for (Entry<TableName, Map<byte[], RowMutation>> tableEntry : mutations.entrySet()) {
      for (Entry<byte[], RowMutation> rowEntry : tableEntry.getValue().entrySet()) {
        ++rowCount;
        columnCount += rowEntry.getValue().size();
      }
    }
    return new Pair<Integer, Integer>(rowCount, columnCount);
  }

  public int size() {
    return mutations.values().stream().flatMap(e -> e.values().stream()).mapToInt(e -> e.size())
      .sum();
  }

  public boolean hasMutation(ColumnCoordinate columnCoordinate) {
    return getMutation(columnCoordinate) != null;
  }

  public Pair<Type, byte[]> getMutation(ColumnCoordinate column) {
    Map<byte[], RowMutation> tableMutation = mutations.get(column.getTableName());
    if (tableMutation != null) {
      RowMutation rowMutation = tableMutation.get(column.getRow());
      if (rowMutation != null) {
        return rowMutation.getMutation(column);
      }
    }
    return null;
  }

  public Type getType(ColumnCoordinate columnCoordinate) {
    Pair<Type, byte[]> mutation = getMutation(columnCoordinate);
    return mutation == null ? null : mutation.getFirst();
  }

  public RowMutation getRowMutation(TableAndRow tableAndRow) {
    return getRowMutation(tableAndRow.getTableName(), tableAndRow.getRowkey());
  }

  public RowMutation getRowMutation(TableName tableName, byte[] rowkey) {
    Map<byte[], RowMutation> rowMutations = mutations.get(tableName);
    return rowMutations == null ? null : rowMutations.get(rowkey);
  }
}