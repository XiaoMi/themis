package com.xiaomi.infra.themis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle.LocalTimestampOracle;
import org.apache.hadoop.hbase.util.Pair;

public class LocalTimestampWithFail extends LocalTimestampOracle {
  public static final String TIMESTAMP_SERVER_EXCEPTION_MESSAGE = "LocalTimestampWithFail Exception";
  
  public LocalTimestampWithFail(Configuration conf) throws IOException {
    super(conf);
  }
  
  @Override
  public Pair<Long, Long> getRequestIdWithTimestamp() throws IOException {
    Pair<Long, Long> result = super.getRequestIdWithTimestamp();
    IncrementalTimestamp.randomGenerateException(result.getSecond());
    return result;
  }
}
