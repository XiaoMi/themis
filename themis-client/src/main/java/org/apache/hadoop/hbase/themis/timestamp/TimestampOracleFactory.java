package org.apache.hadoop.hbase.themis.timestamp;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.TransactionConstant;

public class TimestampOracleFactory {
  private static final Log LOG = LogFactory.getLog(TimestampOracleFactory.class);
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
            LOG.fatal("create timestamp oracle fail, timestampOracleClsName=" + timestampOracleClsName, e);
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