package com.xiaomi.infra.themis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.timestamp.RemoteTimestampOracleProxy;
import org.apache.hadoop.hbase.util.Pair;

public class RemoteTimestampWithFail extends RemoteTimestampOracleProxy {
  public RemoteTimestampWithFail(Configuration conf) throws IOException {
    super(conf);
  }

  @Override
  public Pair<Long, Long> getRequestIdWithTimestamp() throws IOException {
    Pair<Long, Long> result = super.getRequestIdWithTimestamp();
    IncrementalTimestamp.randomGenerateException(result.getSecond());
    return result;
  }
}
