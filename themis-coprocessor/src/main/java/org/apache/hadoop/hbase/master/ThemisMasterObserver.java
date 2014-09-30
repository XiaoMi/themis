package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost.MasterEnvironment;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.ServerLockCleaner;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpointClient;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

public class ThemisMasterObserver extends BaseMasterObserver {
  private static final Log LOG = LogFactory.getLog(ThemisMasterObserver.class);

  public static final String THEMIS_EXPIRED_TIMESTAMP_CALCULATE_PERIOD_KEY = "themis.expired.timestamp.calculator.period";
  public static final String THEMIS_ENABLE_KEY = "THEMIS_ENABLE";
  public static final String THEMIS_EXPIRED_TIMESTAMP_CALCULATE_ENABLE_KEY = "themis.expired.timestamp.calculate.enable";
  public static final String THEMIS_EXPIRED_TIMESTAMP_ZNODE_NAME = "themis-expired-ts";

  protected boolean expiredTimestampCalculateEnable;

  protected int expiredTsCalculatePeriod;
  protected Chore themisExpiredTsCalculator;
  protected ZooKeeperWatcher zk;
  protected String themisExpiredTsZNodePath;
  protected HConnection connection;
  protected ThemisEndpointClient cpClient;
  protected ServerLockCleaner lockCleaner;
  
  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    expiredTimestampCalculateEnable = ctx.getConfiguration().getBoolean(
      THEMIS_EXPIRED_TIMESTAMP_CALCULATE_ENABLE_KEY, true);
    if (expiredTimestampCalculateEnable) {
      startExpiredTimestampCalculator((MasterEnvironment)ctx);
    }
  }
  
  @Override
  public void stop(CoprocessorEnvironment ctx) throws IOException {
    if (connection != null) {
      connection.close();
    }
  }
  
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    boolean themisEnable = false;
    for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
      if (isThemisEnableFamily(columnDesc)) {
        themisEnable = true;
        break;
      }
    }
    
    if (themisEnable) {
      for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
        if (Bytes.equals(ColumnUtil.LOCK_FAMILY_NAME, columnDesc.getName())) {
          throw new DoNotRetryIOException("family '" + ColumnUtil.LOCK_FAMILY_NAME_STRING
              + "' is preserved by themis when " + THEMIS_ENABLE_KEY
              + " is true, please change your family name");
        }
        // make sure TTL and MaxVersion is not set by user
        if (columnDesc.getTimeToLive() != HConstants.FOREVER) {
          throw new DoNotRetryIOException("can not set TTL for family '"
              + columnDesc.getNameAsString() + "' when " + THEMIS_ENABLE_KEY + " is true, TTL="
              + columnDesc.getTimeToLive());
        }
        if (columnDesc.getMaxVersions() != HColumnDescriptor.DEFAULT_VERSIONS
            && columnDesc.getMaxVersions() != Integer.MAX_VALUE) {
          throw new DoNotRetryIOException("can not set MaxVersion for family '"
              + columnDesc.getNameAsString() + "' when " + THEMIS_ENABLE_KEY
              + " is true, MaxVersion=" + columnDesc.getMaxVersions());
        }
        columnDesc.setMaxVersions(Integer.MAX_VALUE);
      }
      desc.addFamily(createLockFamily());
      LOG.info("add family '" + ColumnUtil.LOCK_FAMILY_NAME_STRING + "' for table:" + desc.getNameAsString());
    }    
  }
  
  protected static HColumnDescriptor createLockFamily() {
    HColumnDescriptor desc = new HColumnDescriptor(ColumnUtil.LOCK_FAMILY_NAME);
    desc.setInMemory(true);
    desc.setMaxVersions(1);
    desc.setTimeToLive(HConstants.FOREVER);
    // TODO(cuijianwei) : choose the best bloom filter type
    // desc.setBloomFilterType(BloomType.ROWCOL);
    return desc;
  }
  
  public static boolean isThemisEnableFamily(HColumnDescriptor desc) {
    String value = desc.getValue(THEMIS_ENABLE_KEY);
    return value != null && Boolean.parseBoolean(value);
  }
  
  public static boolean isThemisEnableTable(HTableDescriptor desc) {
    for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
      if (isThemisEnableFamily(columnDesc)) {
        return true;
      }
    }
    return false;
  }
  
  protected synchronized void startExpiredTimestampCalculator(MasterEnvironment ctx)
      throws IOException {
    if (themisExpiredTsCalculator != null) {
      return;
    }

    LOG.info("try to start thread to compute the expired timestamp for themis table");
    this.zk = ((MasterEnvironment)ctx).getMasterServices().getZooKeeper();
    themisExpiredTsZNodePath = getThemisExpiredTsZNodePath(this.zk);

    connection = HConnectionManager.createConnection(ctx.getConfiguration());
    cpClient = new ThemisEndpointClient(connection);
    lockCleaner = new ServerLockCleaner(connection, cpClient);
    
    TransactionTTL.init(ctx.getConfiguration());
    String expiredTsCalculatePeriodStr = ctx.getConfiguration()
        .get(THEMIS_EXPIRED_TIMESTAMP_CALCULATE_PERIOD_KEY);
    if (expiredTsCalculatePeriodStr == null) {
      expiredTsCalculatePeriod = TransactionTTL.readTransactionTTL / 1000;
    } else {
      expiredTsCalculatePeriod = Integer.parseInt(expiredTsCalculatePeriodStr);
    }
    
    themisExpiredTsCalculator = new ExpiredTimestampCalculator(expiredTsCalculatePeriod * 1000,
        ctx.getMasterServices());
    Threads.setDaemonThreadRunning(themisExpiredTsCalculator.getThread());
    LOG.info("successfully start thread to compute the expired timestamp for themis table, timestampType="
        + TransactionTTL.timestampType + ", calculatorPeriod=" + expiredTsCalculatePeriod + ", zkPath="
        + themisExpiredTsZNodePath);
  }
  
  class ExpiredTimestampCalculator extends Chore {
    private long currentExpiredTs;

    public ExpiredTimestampCalculator(int p, MasterServices service) {
      super("ThemisExpiredTimestampCalculator", p, service);
    }
    
    @Override
    protected void chore() {
      long currentMs = System.currentTimeMillis();
      currentExpiredTs = TransactionTTL.getExpiredTimestampForReadByDataColumn(currentMs);
      LOG.info("computed expired timestamp for themis, currentExpiredTs=" + currentExpiredTs
          + ", currentMs=" + currentMs);
      try {
        cleanLockBeforeTimestamp(currentExpiredTs);
        setExpiredTsToZk(currentExpiredTs);
      } catch (Exception e) {
        LOG.error("themis clean expired lock fail", e);
      }
     }
    
    public long getCurrentExpiredTs() {
      return currentExpiredTs;
    }
  }
  
  public static List<String> getThemisTables(HConnection connection) throws IOException {
    List<String> tableNames = new ArrayList<String>();
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(connection);
      HTableDescriptor[] tables = admin.listTables();
      for (HTableDescriptor table : tables) {
        if (isThemisEnableTable(table)) {
          tableNames.add(table.getNameAsString());
        }
      }
    } catch (IOException e) {
      LOG.error("get table names fail", e);
      throw e;
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
    return tableNames;
  }
  
  public static String getThemisExpiredTsZNodePath(ZooKeeperWatcher zk) {
    return zk.baseZNode + "/" + THEMIS_EXPIRED_TIMESTAMP_ZNODE_NAME;
  }
  
  public static long getThemisExpiredTsFromZk(ZooKeeperWatcher zk) throws Exception {
    return getThemisExpiredTsFromZk(zk, getThemisExpiredTsZNodePath(zk));
  }
  
  public static long getThemisExpiredTsFromZk(ZooKeeperWatcher zk, String path) throws Exception {
    byte[] data = ZKUtil.getData(zk, path);
    if (data == null) {
      return Long.MIN_VALUE;
    }
    return Long.parseLong(Bytes.toString(data));
  }
  
  public void setExpiredTsToZk(long currentExpiredTs) throws Exception {
    ZKUtil.createSetData(zk, themisExpiredTsZNodePath,
      Bytes.toBytes(String.valueOf(currentExpiredTs)));
    LOG.info("successfully set currentExpiredTs to zk, currentExpiredTs=" + currentExpiredTs
        + ", zkPath=" + themisExpiredTsZNodePath);
  }
  
  public void cleanLockBeforeTimestamp(long ts) throws IOException {
    List<String> tableNames = getThemisTables(connection);
    for (String tableName : tableNames) {
      LOG.info("start to clean expired lock for themis table:" + tableName);
      byte[] tableNameBytes = Bytes.toBytes(tableName);
      HTableInterface hTable = null;
      int cleanedLockCount = 0;
      try {
        hTable = new HTable(connection.getConfiguration(), tableName);
        Scan scan = new Scan();
        scan.addFamily(ColumnUtil.LOCK_FAMILY_NAME);
        scan.setTimeRange(0, ts);
        ResultScanner scanner = hTable.getScanner(scan);
        Result result = null;
        while ((result = scanner.next()) != null) {
          for (KeyValue kv : result.list()) {
            ThemisLock lock = ThemisLock.parseFromByte(kv.getValue());
            Column dataColumn = ColumnUtil.getDataColumnFromLockColumn(new Column(kv.getFamily(),
                kv.getQualifier()));
            lock.setColumn(new ColumnCoordinate(tableNameBytes, kv.getRow(),
                dataColumn.getFamily(), dataColumn.getQualifier()));
            lockCleaner.cleanLock(lock);
            ++cleanedLockCount;
            LOG.info("themis clean expired lock, lockTs=" + kv.getTimestamp() + ", expiredTs=" + ts
                + ", lock=" + ThemisLock.parseFromByte(kv.getValue()));
          }
        }
        scanner.close();
      } finally {
        if (hTable != null) {
          hTable.close();
        }
      }
      LOG.info("finish clean expired lock for themis table:" + tableName + ", cleanedLockCount="
          + cleanedLockCount);
    }
  }
}
