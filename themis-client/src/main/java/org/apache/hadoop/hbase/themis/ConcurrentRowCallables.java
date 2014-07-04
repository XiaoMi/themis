package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.themis.ConcurrentRowCallables.TableAndRow;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.util.Bytes;

abstract class RowCallable<R> implements Callable<R> {
  private TableAndRow tableAndRow;

  public RowCallable(byte[] tableName, byte[] rowkey) {
    this.tableAndRow = new TableAndRow(tableName, rowkey);
  }

  public TableAndRow getTableAndRow() {
    return this.tableAndRow;
  }
}

public class ConcurrentRowCallables<R> {
  public static class TableAndRow implements Comparable<TableAndRow> {
    private byte[] tableName;
    private byte[] row;
    
    public TableAndRow(byte[] tableName, byte[] row) {
      this.tableName = tableName;
      this.row = row;
    }

    public byte[] getTableName() {
      return this.tableName;
    }
    
    public byte[] getRowkey() {
      return this.row;
    }
    
    @Override
    public int compareTo(TableAndRow other) {
      int cmp = Bytes.compareTo(tableName, other.tableName);
      if (cmp == 0) {
        return Bytes.compareTo(row, other.row);
      }
      return cmp;
    }
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      if (tableName != null) {
        result = prime * result + Bytes.toString(tableName).hashCode();
      }
      if (row != null) {
        result = prime * result + Bytes.toString(row).hashCode();
      }
      return result;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof TableAndRow)) {
        return false;
      }
      TableAndRow tableAndRow = (TableAndRow) other;
      return Bytes.equals(this.getTableName(), tableAndRow.getTableName())
          && Bytes.equals(this.getRowkey(), tableAndRow.getRowkey());
    }
    
    public String toString() {
      return "tableName=" + Bytes.toString(tableName) + "/rowkey=" + Bytes.toString(row);
    }
  }


  private final ExecutorService threadPool;
  Map<TableAndRow, Future<R>> futureMaps = new TreeMap<TableAndRow, Future<R>>();
  Map<TableAndRow, R> resultMaps = new TreeMap<TableAndRow, R>();
  Map<TableAndRow, IOException> exceptionMaps = new TreeMap<TableAndRow, IOException>();

  public ConcurrentRowCallables(ExecutorService threadPool) {
    this.threadPool = threadPool;
  }

  public void addCallable(RowCallable<R> callable) throws IOException {
    TableAndRow tableAndRow = callable.getTableAndRow();
    if (this.futureMaps.containsKey(tableAndRow) || this.exceptionMaps.containsKey(tableAndRow)) {
      throw new ThemisFatalException("add duplicated row callable, tableAndRow=" + tableAndRow);
    }
    try {
      Future<R> future = this.threadPool.submit(callable);
      this.futureMaps.put(tableAndRow, future);
    } catch (Throwable e) {
      exceptionMaps.put(tableAndRow, new IOException(e));
    }
  }

  public void waitForResult() {
    for (Entry<TableAndRow, Future<R>> entry : this.futureMaps.entrySet()) {
      TableAndRow tableAndRow = entry.getKey();
      try {
        R result = entry.getValue().get();
        this.resultMaps.put(tableAndRow, result);
      } catch (Exception e) {
        this.exceptionMaps.put(tableAndRow, new IOException(e));
      }
    }
  }

  public Map<TableAndRow, R> getResults() {
    return resultMaps;
  }
  
  public R getResult(byte[] tableName, byte[] rowkey) {
    return resultMaps.get(new TableAndRow(tableName, rowkey));
  }

  public Map<TableAndRow, IOException> getExceptions() {
    return exceptionMaps;
  }
  
  public IOException getException(byte[] tableName, byte[] rowkey) {
    return exceptionMaps.get(new TableAndRow(tableName, rowkey));
  }
}