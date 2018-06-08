package org.apache.hadoop.hbase.themis.columns;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

// index mutations of the same row by columns
public class RowMutation {
  private byte[] row;
  private Map<Column, Pair<Type, byte[]>> mutations = new TreeMap<Column, Pair<Type,byte[]>>();

  public RowMutation(byte[] row) {
    this.row = row;
  }
  
  public boolean addMutation(byte[] family, byte[] qualifier, Type type, byte[] value) {
    return addMutation(new Column(family, qualifier), type, value);
  }
  
  public boolean addMutation(byte[] family, byte[] qualifier, byte type, byte[] value) {
    return addMutation(family, qualifier, Type.codeToType(type), value);
  }
  
  public boolean addMutation(Column column, Type type, byte[] value) {
    boolean contained = mutations.containsKey(column);
    mutations.put(column, new Pair<Type, byte[]>(type, value));
    return !contained;
  }

  public void removeMutation(Column column) {
    if (mutations != null && !mutations.isEmpty()) {
      mutations.remove(column);
    }
  }
  
  public List<ColumnMutation> mutationList() {
    return mutationList(true);
  }

  public Set<Column> getColumns() {
    return mutations.keySet();
  }
  
  public List<ColumnMutation> mutationList(boolean withValue) {
    List<ColumnMutation> rowMutations = new ArrayList<ColumnMutation>();
    for (Entry<Column, Pair<Type, byte[]>> entry : mutations.entrySet()) {
      ColumnMutation mutation = new ColumnMutation(entry.getKey(), entry.getValue().getFirst(),
          withValue ? entry.getValue().getSecond() : null);
      rowMutations.add(mutation);
    }
    return rowMutations;
  }
  
  // only return columns with corresponding types
  public List<ColumnMutation> mutationListWithoutValue() {
    return mutationList(false);
  }
  
  public boolean hasMutation(Column column) {
    return mutations.containsKey(column);
  }
  
  public boolean hasMutation(byte[] family, byte[] qualifier) {
    return hasMutation(new Column(family, qualifier));
  }
  
  public int size() {
    return mutations.size();
  }
  
  public byte[] getRow() {
    return row;
  }
  
  public Type getType(Column column) {
    Pair<Type, byte[]> mutation = getMutation(column);
    if (mutation != null) {
      return mutation.getFirst();
    }
    return null;
  }
  
  public Pair<Type, byte[]> getMutation(Column column) {
    return mutations.get(column);
  }

  @Override
  public String toString() {
    String result = Bytes.toString(row) + "\n";
    List<ColumnMutation> mutationList = mutationList();
    for (ColumnMutation mutation : mutationList) {
      result += ("columnMutation=" + mutation);
    }
    return result;
  }
}