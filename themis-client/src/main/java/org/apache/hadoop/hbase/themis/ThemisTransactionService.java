package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import com.xiaomi.infra.hbase.client.HConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.transaction.NotSupportedException;
import org.apache.hadoop.hbase.transaction.Transaction;
import org.apache.hadoop.hbase.transaction.TransactionIsolationLevel;
import org.apache.hadoop.hbase.transaction.TransactionService;
import org.apache.hadoop.hbase.transaction.TransactionType;

import com.xiaomi.common.perfcounter.PerfCounter;
import com.xiaomi.infra.hbase.client.HException;
import com.xiaomi.infra.hbase.client.InternalHBaseClient;
import com.xiaomi.miliao.counter.MultiCounter;

public class ThemisTransactionService extends TransactionService {
  private static final Log LOG = LogFactory.getLog(ThemisTransactionService.class);
  public static final String THEMIS_PERFCOUNT_PRFEIX = "themis";
  private static int slowAccessCutoff = 100;
  private static String clusterName = "";
  
  private HConnection connection;

  public ThemisTransactionService(String configPath) throws HException {
    this(HConfigUtil.loadConfiguration(configPath));
    slowAccessCutoff = this.config.getInt(HConfigUtil.HBASE_SLOW_ACCESS_CUTOFF,
        HConfigUtil.DEFAULT_HBASE_SLOW_ACCESS_CUTOFF);
    clusterName = this.config.get(HConfigUtil.HBASE_CLUSTER_NAME);
  }
  
  public ThemisTransactionService(Configuration conf) throws HException {
    super(conf);
    String clusterName = conf.get(HConfigUtil.HBASE_CLUSTER_NAME, "");
    try {
      if (clusterName.length() == 0) {
        connection = HConnectionManager.createConnection(conf);
      } else {
        connection = HConnectionManager.createConnection(conf, "hbase://" + clusterName);
      }
    } catch (IOException e) {
      throw new HException(e);
    }
  }

  public HConnection getHConnection() {
    return connection;
  }
  
  public static void logHBaseSlowAccess(String methodName, long timeConsume) {
    if (timeConsume >= slowAccessCutoff) {
      LOG.warn("themis slow access, method=" + methodName + ", timeconsume=" + timeConsume);
    }
  }
  
  public static String constructClusterAndMethodPerfcountName(String methodName) {
    return THEMIS_PERFCOUNT_PRFEIX + "-" + clusterName + "-" + methodName;
  }
  
  public static String constructClusterAndMethodFailPerfcountName(String methodName) {
    return constructClusterAndMethodPerfcountName(methodName) + MultiCounter.FAIL_SUFFIX;
  }
  
  public static void addCounter(String method, long time) {
    PerfCounter.count(constructClusterAndMethodPerfcountName(method), 1, time);
  }
  
  public static void addFailCounter(String method) {
    PerfCounter.count(constructClusterAndMethodFailPerfcountName(method), 1);
  }
  
  @Override
  public Transaction[] getAll() throws HException {
    throw new HException(new NotSupportedException());
  }

  @Override
  public void setIsolationLevel(TransactionIsolationLevel isolationLevel) throws HException {
    throw new HException(new NotSupportedException());
  }

  @Override
  public void setTransactionType(TransactionType transactionType) throws HException {
    throw new HException(new NotSupportedException());
  }

  @Override
  public void setTransactionTimeout(int timeoutSeconds) throws HException {
    throw new HException(new NotSupportedException());
  }

  @Override
  public TransactionIsolationLevel[] getSupportedIsolationLevels() throws HException {
    throw new HException(new NotSupportedException());
  }
  
  public void close() throws HException {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (IOException e) {
      throw new HException(e);
    }
  }
}