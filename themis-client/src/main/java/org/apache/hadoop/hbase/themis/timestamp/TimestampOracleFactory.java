package org.apache.hadoop.hbase.themis.timestamp;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampOracleFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TimestampOracleFactory.class);
  private static volatile BaseTimestampOracle timestampClient = null;
  private static Object timestampClientLock = new Object();

  public static BaseTimestampOracle getTimestampOracle(Configuration conf) throws IOException {
    String timestampOracleClsName = conf.get(TransactionConstant.TIMESTAMP_ORACLE_CLASS_KEY,
      TransactionConstant.DEFAULT_TIMESTAMP_ORACLE_CLASS);
    if (timestampClient == null) {
      synchronized (timestampClientLock) {
        if (timestampClient == null) {
          try {
            timestampClient = (BaseTimestampOracle) (Class.forName(timestampOracleClsName)
              .getConstructor(Configuration.class).newInstance(conf));
          } catch (Throwable e) {
            LOG.error(
              "create timestamp oracle fail, timestampOracleClsName=" + timestampOracleClsName, e);
            throw new IOException(e);
          }
        }
      }
    }
    return timestampClient;
  }

  public static BaseTimestampOracle getTimestampOracleWithoutCreate() {
    return timestampClient;
  }
}