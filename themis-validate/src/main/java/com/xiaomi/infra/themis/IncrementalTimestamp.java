package com.xiaomi.infra.themis;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle;
import org.apache.hadoop.hbase.util.Pair;

import com.xiaomi.infra.themis.exception.GeneratedTestException;

public class IncrementalTimestamp extends BaseTimestampOracle {
  private static final Log LOG = LogFactory.getLog(IncrementalTimestamp.class);
  public static final String TIMESTAMP_SERVER_EXCEPTION_MESSAGE = "IncrementalTimestamp Exception";
  private static long timestamp = 0;
  private static Object lock = new Object();
  private static Random rd = new Random();
  
  public IncrementalTimestamp(Configuration conf) throws IOException {
    super(conf);
  }
  
  public Pair<Long, Long> getRequestIdWithTimestamp() throws IOException {
    long result = 0;
    synchronized (lock) {
      result = ++timestamp;
    }
    randomGenerateException(result);
    return new Pair<Long, Long>(result, result);
  }
  
  public static void randomGenerateException(long result) throws GeneratedTestException {
    if (rd.nextInt(20000) == 0) {
      LOG.warn("throw exception when get timestamp=" + result);
      throw new GeneratedTestException(TIMESTAMP_SERVER_EXCEPTION_MESSAGE);
    }    
  }

  @Override
  public long getTimestamps(int n) throws IOException {
    throw new IOException("unsupport method invoke");
  }
}
