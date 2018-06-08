package org.apache.hadoop.hbase.themis;

import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.index.Indexer.NullIndexer;
import org.apache.hadoop.hbase.themis.lockcleaner.WorkerRegister.NullWorkerRegister;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle.LocalTimestampOracle;

public class TransactionConstant {
  public static final String TIMESTAMP_ORACLE_CLASS_KEY = "themis.timestamp.oracle.class";
  public static final String DEFAULT_TIMESTAMP_ORACLE_CLASS = LocalTimestampOracle.class.getName();
  public static final String WORKER_REGISTER_CLASS_KEY = "themis.worker.register.class";
  public static final String DEFAULT_WORKER_REISTER_CLASS = NullWorkerRegister.class.getName();
  public static final String THEMIS_RETRY_COUNT = "themis.retry.count";
  public static final int DEFAULT_THEMIS_RETRY_COUNT = 10;
  public static final String THEMIS_PAUSE = "themis.pause";
  public static final int DEFAULT_THEMIS_PAUSE = 100;
  public static final String THEMIS_ENABLE_CONCURRENT_RPC = "themis.enable.concurrent.rpc";
  
  // constants for timestamp oracle
  public static final String MAX_TIMESTAMP_REQUEST_QUEUE_KEY = "themis.max.timestamp.request.queue.size";
  public static final int DEFAULT_MAX_TIMESTAMP_REQUEST_QUEUE_LEN = 10000;
  public static final String TIMESTAMP_REQUEST_TIMEOUT = "themis.timestamp.request.timeout";
  public static final int DEFAULT_TIMESTAMP_REQUEST_TIMEOUT = 1000;
  // zookeeper quorum where remote timestamp server registered
  public static final String REMOTE_TIMESTAMP_SERVER_ZK_QUORUM_KEY = "themis.remote.timestamp.server.zk.quorum";
  public static final String DEFAULT_REMOTE_TIMESTAMP_SERVER_ZK_QUORUM = "127.0.0.1:2181";
  public static final String REMOTE_TIMESTAMP_SERVER_CLUSTER_NAME = "themis.remote.timestamp.server.clustername";
  public static final String DEFAULT_REMOTE_TIMESTAMP_SERVER_CLUSTER = "default-cluster";
  // secondary index
  public static final String INDEXER_CLASS_KEY = "themis.indexer.class";
  public static final String DEFAULT_INDEXER_CLASS = NullIndexer.class.getName();
  // lock clean
  public static final String CLIENT_LOCK_TTL_KEY = "themis.client.lock.clean.ttl";
  public static final int DEFAULT_CLIENT_LOCK_TTL = 0;
  
  public static final String ENABLE_SINGLE_ROW_WRITE = "themis.enable.singlerow.write";
  public static final String DISABLE_LOCK_CLEAN = "themis.disable.lock.clean";

  // auxiliary family and qualifier
  public static final String AUXILIARY_FAMILY_NAME_KEY = ColumnUtil.AUXILIARY_FAMILY_NAME_KEY;
  public static final String DEFAULT_AUXILIARY_FAMILY_NAME = ColumnUtil.DEFAULT_AUXILIARY_FAMILY_NAME;
  public static final String AUXILIARY_QUALIFIER_NAME_KEY = ColumnUtil.AUXILIARY_QUALIFIER_NAME_KEY;
  public static final String DEFAULT_AUXILIARY_QUALIFIER_NAME = ColumnUtil.DEFAULT_AUXILIARY_QUALIFIER_NAME;
}
