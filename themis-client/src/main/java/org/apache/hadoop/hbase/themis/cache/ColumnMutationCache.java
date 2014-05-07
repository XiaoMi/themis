package org.apache.hadoop.hbase.themis.cache;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.themis.ConcurrentRowCallables.TableAndRow;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

// local cache themis transaction
public class ColumnMutationCache {
  // index mutations by table and row
  private Map<byte[], Map<byte[], RowMutation>> mutations =
      new TreeMap<byte[], Map<byte[], RowMutation>>(Bytes.BYTES_COMPARATOR); 
  
  public boolean addMutation(byte[] tableName, KeyValue kv) {
    Map<byte[], RowMutation> rowMutations = mutations.get(tableName);
    if (rowMutations == null) {
      rowMutations = new TreeMap<byte[], RowMutation>(Bytes.BYTES_COMPARATOR);
      mutations.put(tableName, rowMutations);
    }
    RowMutation rowMutation = rowMutations.get(kv.getRow());
    if (rowMutation == null) {
      rowMutation = new RowMutation(kv.getRow());
      rowMutations.put(kv.getRow(), rowMutation);
    }
    return rowMutation.addMutation(kv.getFamily(), kv.getQualifier(), kv.getType(), kv.getValue());
  }
  
  public Set<Entry<byte[], Map<byte[], RowMutation>>> getMutations() {
    return mutations.entrySet();
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
  
  public RowMutation getRowMutation(byte[] tableName, byte[] rowkey) {
    Map<byte[], RowMutation> rowMutations = mutations.get(tableName);
    return rowMutations == null ? null : rowMutations.get(rowkey);
  }
}