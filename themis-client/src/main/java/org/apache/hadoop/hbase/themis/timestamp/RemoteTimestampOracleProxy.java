package org.apache.hadoop.hbase.themis.timestamp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.ThemisStatistics;
import org.apache.hadoop.hbase.themis.TransactionConstant;

import com.xiaomi.infra.chronos.client.ChronosClient;

// a remote implementation of timestamp oracle to query incremental timestamp server(ITS)
public class RemoteTimestampOracleProxy extends BaseTimestampOracle {
  private final ChronosClient clientImpl;
  public RemoteTimestampOracleProxy(Configuration conf) throws IOException {
    super(conf);
    String zkQurom = conf.get(TransactionConstant.REMOTE_TIMESTAMP_SERVER_ZK_QUORUM_KEY,
      TransactionConstant.DEFAULT_REMOTE_TIMESTAMP_SERVER_ZK_QUORUM);
    String clusterName = conf.get(TransactionConstant.REMOTE_TIMESTAMP_SERVER_CLUSTER_NAME,
      TransactionConstant.DEFAULT_REMOTE_TIMESTAMP_SERVER_CLUSTER);
    clientImpl = new ChronosClient(zkQurom, clusterName);
  }

  public long getTimestamps(int n) throws IOException {
    long beginTs = System.nanoTime();
    try {
      return clientImpl.getTimestamps(n);
    } finally {
      ThemisStatistics.updateLatency(ThemisStatistics.getStatistics().remoteTimestampRequestLatency, beginTs);
    }
  }
  
  @Override
  public void close() throws IOException {
    if (clientImpl != null) {
      clientImpl.close();
    }
    super.close();
  }
}