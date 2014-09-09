package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpointClient;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;

// a wrapped client of ThemisCoprocessorClient which computes latency of key methods
public class WrappedCoprocessorClient extends ThemisEndpointClient {
  public WrappedCoprocessorClient(HConnection connection) {
    super(connection);
  }
  
  @Override
  public Result themisGet(final byte[] tableName, final Get get, final long startTs,
      final boolean ignoreLock) throws IOException {
    long beginTs = System.nanoTime();
    try {
      return super.themisGet(tableName, get, startTs, ignoreLock);
    } finally {
      ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().readLatency, beginTs);
    }
  }
  
  @Override
  public ThemisLock prewriteRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
      final byte[] secondaryLock, final int primaryIndex) throws IOException {
    long beginTs = System.nanoTime();
    try {
      return super.prewriteRow(tableName, row, mutations, prewriteTs, primaryLock, secondaryLock, primaryIndex);
    } finally {
      ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().prewriteLatency,
        beginTs);
    }
  }
  
  @Override
  public ThemisLock prewriteSingleRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final byte[] primaryLock,
      final byte[] secondaryLock, final int primaryIndex) throws IOException {
    long beginTs = System.nanoTime();
    try {
      return super.prewriteSingleRow(tableName, row, mutations, prewriteTs, primaryLock,
        secondaryLock, primaryIndex);
    } finally {
      ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().prewriteLatency,
        beginTs);
    }
  }
  
  @Override
  public void commitRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final long commitTs,
      final int primaryIndex) throws IOException {
    long beginTs = System.nanoTime();
    try {
      super.commitRow(tableName, row, mutations, prewriteTs, commitTs, primaryIndex);
    } finally {
      if (primaryIndex >= 0) {
        ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().commitPrimaryLatency,
          beginTs);
      } else {
        ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().commitSecondaryLatency,
          beginTs);        
      }
    }
  }
  
  @Override
  public void commitSingleRow(final byte[] tableName, final byte[] row,
      final List<ColumnMutation> mutations, final long prewriteTs, final long commitTs,
      final int primaryIndex) throws IOException {
    long beginTs = System.nanoTime();
    try {
      super.commitSingleRow(tableName, row, mutations, prewriteTs, commitTs, primaryIndex);
    } finally {
      if (primaryIndex >= 0) {
        ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().commitPrimaryLatency,
          beginTs);
      } else {
        ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().commitSecondaryLatency,
          beginTs);        
      }
    }
  }
}