package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.util.Bytes;

abstract class RowCallable<R> implements Callable<R> {
  private byte[] rowkey;

  public RowCallable(byte[] rowkey) {
    this.rowkey = rowkey;
  }

  public byte[] getRowkey() {
    return rowkey;
  }
}

public class ConcurrentRowCallables<R> {
  private final ExecutorService threadPool;
  Map<byte[], Future<R>> futureMaps = new TreeMap<byte[], Future<R>>(Bytes.BYTES_COMPARATOR);
  Map<byte[], R> resultMaps = new TreeMap<byte[], R>(Bytes.BYTES_COMPARATOR);
  Map<byte[], IOException> exceptionMaps = new TreeMap<byte[], IOException>(Bytes.BYTES_COMPARATOR);

  public ConcurrentRowCallables(ExecutorService threadPool) {
    this.threadPool = threadPool;
  }

  public void addCallable(RowCallable<R> callable) {
    try {
      Future<R> future = this.threadPool.submit(callable);
      this.futureMaps.put(callable.getRowkey(), future);
    } catch (Throwable e) {
      exceptionMaps.put(callable.getRowkey(), new IOException(e));
    }
  }

  public void waitForResult() {
    for (Entry<byte[], Future<R>> entry : this.futureMaps.entrySet()) {
      byte[] rowkey = entry.getKey();
      try {
        R result = entry.getValue().get();
        this.resultMaps.put(rowkey, result);
      } catch (Exception e) {
        this.exceptionMaps.put(rowkey, new IOException(e));
      }
    }
  }

  public Map<byte[], R> getResults() {
    return resultMaps;
  }

  public Map<byte[], IOException> getExceptions() {
    return exceptionMaps;
  }
}