package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.themis.config.NameService;
import org.apache.hadoop.hbase.themis.config.NameServiceEntry;
import org.apache.hadoop.hbase.themis.exception.HException;
import org.apache.hadoop.hbase.themis.util.ConfigUtils;
import org.apache.hadoop.hbase.themis.util.ConnectionUtils;
import org.apache.hadoop.hbase.transaction.NotSupportedException;
import org.apache.hadoop.hbase.transaction.Transaction;
import org.apache.hadoop.hbase.transaction.TransactionIsolationLevel;
import org.apache.hadoop.hbase.transaction.TransactionService;
import org.apache.hadoop.hbase.transaction.TransactionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.client.ConnectionFactory.createConnection;

public class ThemisTransactionService extends TransactionService {
  private static final Logger LOG = LoggerFactory.getLogger(ThemisTransactionService.class);
  public static final String THEMIS_PERFCOUNT_PRFEIX = "themis";
  private static int slowAccessCutoff = 100;
  private static String clusterName = "";

  private Connection connection;

  public ThemisTransactionService(String configPath) throws HException {
    this(ConfigUtils.loadConfiguration(configPath));
    slowAccessCutoff = this.config.getInt(ConfigUtils.HBASE_SLOW_ACCESS_CUTOFF,
        ConfigUtils.DEFAULT_HBASE_SLOW_ACCESS_CUTOFF);
    clusterName = this.config.get(ConfigUtils.HBASE_CLUSTER_NAME);
  }

  public ThemisTransactionService(Configuration conf) throws HException {
    super(conf);
    String clusterName = conf.get(ConfigUtils.HBASE_CLUSTER_NAME, "");

    try {
      if (clusterName.length() == 0) {
        connection = createConnection(conf);
      } else {
        final String clusterUri = "hbase://" + clusterName;
        NameServiceEntry entry;

        entry = NameService.resolve(clusterUri, conf);
        if (entry.getScheme() != null && "hbase".equals(entry.getScheme())) {
          conf = entry.createClusterConf(conf);
          connection = ConnectionUtils.createConnection(conf, null, null);
        }
      }
    } catch (IOException e) {
      throw new HException(e);
    }
  }


  @Override
  public Connection getHConnection() {
    return connection;
  }

  public static void logHBaseSlowAccess(String methodName, long timeConsume) {
    if (timeConsume >= slowAccessCutoff) {
      LOG.warn("themis slow access, method=" + methodName + ", timeconsume=" + timeConsume);
    }
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

  @Override
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