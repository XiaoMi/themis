package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.ServerLockCleaner;
import org.apache.hadoop.hbase.themis.cp.ThemisCoprocessorClient;
import org.apache.hadoop.hbase.themis.cp.ThemisCpStatistics;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;


@CoreCoprocessor
public class ThemisMasterObserver implements MasterObserver, MasterCoprocessor {
  private static final Log LOG = LogFactory.getLog(ThemisMasterObserver.class);

  public static final String RETURNED_THEMIS_TABLE_DESC = "__themis.returned.table.desc__"; // support truncate table
  public static final String THEMIS_ENABLE_KEY = "THEMIS_ENABLE";
  public static final String THEMIS_EXPIRED_TIMESTAMP_CALCULATE_PERIOD_KEY = "themis.expired.timestamp.calculator.period";
  public static final String THEMIS_EXPIRED_DATA_CLEAN_ENABLE_KEY = "themis.expired.data.clean.enable";
  public static final String THEMIS_EXPIRED_TIMESTAMP_ZNODE_NAME = "themis-expired-ts";
  public static final String CHORE_THREAD_NAME = "ThemisExpiredTimestampCalculator";
  
  protected int expiredTsCalculatePeriod;
  protected Thread themisExpiredTsCalculator;
  protected ZKWatcher zk;
  protected String themisExpiredTsZNodePath;
  protected Connection connection;
  protected ServerLockCleaner lockCleaner;


  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    ColumnUtil.init(ctx.getConfiguration());
    if (ctx.getConfiguration().getBoolean(THEMIS_EXPIRED_DATA_CLEAN_ENABLE_KEY, true)) {
      TransactionTTL.init(ctx.getConfiguration());
      ThemisCpStatistics.init(ctx.getConfiguration());

      //todo
      assert ctx instanceof HasMasterServices;
      startExpiredTimestampCalculator(
              ((HasMasterServices) ctx).getMasterServices(), ctx.getConfiguration());
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
      TableDescriptor desc, RegionInfo[] regions) throws IOException {
    if (isReturnedThemisTableDesc(desc)) {
      return;
    }
    boolean themisEnable = false;
    for (ColumnFamilyDescriptor columnDesc : desc.getColumnFamilies()) {
      if (isThemisEnableFamily(columnDesc)) {
        themisEnable = true;
        break;
      }
    }
    
    if (themisEnable) {
      for (ColumnFamilyDescriptor columnDesc : desc.getColumnFamilies()) {
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
        if (columnDesc.getMaxVersions() != ColumnFamilyDescriptorBuilder.DEFAULT_MAX_VERSIONS
            && columnDesc.getMaxVersions() != Integer.MAX_VALUE) {
          throw new DoNotRetryIOException("can not set MaxVersion for family '"
              + columnDesc.getNameAsString() + "' when " + THEMIS_ENABLE_KEY
              + " is true, MaxVersion=" + columnDesc.getMaxVersions());
        }
        ((ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor) columnDesc).setMaxVersions(Integer.MAX_VALUE);
      }

      ((TableDescriptorBuilder.ModifyableTableDescriptor) desc).setColumnFamily(createLockFamily());
      LOG.info("add family '" + ColumnUtil.LOCK_FAMILY_NAME_STRING + "' for table:" + desc.getTableName().getNameAsString());
      if (!ColumnUtil.isCommitToSameFamily()) {
        addCommitFamilies(desc);
        LOG.info("add commit family '" + ColumnUtil.PUT_FAMILY_NAME + "' and '"
            + ColumnUtil.DELETE_FAMILY_NAME + "' for table:" + desc.getTableName().getNameAsString());
      }
    }    
  }
  
  protected static void setReturnedThemisTableDesc(TableDescriptor desc) {
    ((TableDescriptorBuilder.ModifyableTableDescriptor) desc).setValue(RETURNED_THEMIS_TABLE_DESC, "true");
  }
  
  protected static boolean isReturnedThemisTableDesc(TableDescriptor desc) {
    return desc.getValue(RETURNED_THEMIS_TABLE_DESC) != null
        && Boolean.parseBoolean(desc.getValue(RETURNED_THEMIS_TABLE_DESC));
  }
  
  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList,
                               List<TableDescriptor> descriptors, String regex) throws IOException {
      for (TableDescriptor desc : descriptors) {
          setReturnedThemisTableDesc(desc);
      }
  }
  
  protected static ColumnFamilyDescriptor createLockFamily() {
    ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder
            .newBuilder(ColumnUtil.LOCK_FAMILY_NAME)
            .setMaxVersions(1)
            .setInMemory(true)
            .setTimeToLive(ColumnFamilyDescriptorBuilder.DEFAULT_TTL);
    // TODO(cuijianwei) : choose the best bloom filter type
    // desc.setBloomFilterType(BloomType.ROWCOL);
    return builder.build();
  }

  protected static void addCommitFamilies(TableDescriptor desc) {
    for (byte[] family : ColumnUtil.COMMIT_FAMILY_NAME_BYTES) {
        ((TableDescriptorBuilder.ModifyableTableDescriptor) desc)
                .setColumnFamily(getCommitFamily(family));
    }
  }
  
  protected static ColumnFamilyDescriptor getCommitFamily(byte[] familyName) {
    ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder
        .newBuilder(familyName)
        .setMaxVersions(Integer.MAX_VALUE)
        .setInMemory(true)
        .setTimeToLive(ColumnFamilyDescriptorBuilder.DEFAULT_TTL);
    return builder.build();
  }
  
  public static boolean isThemisEnableFamily(ColumnFamilyDescriptor desc) {
    String value = Bytes.toString(desc.getValue(Bytes.toBytes(THEMIS_ENABLE_KEY)));
    return Boolean.parseBoolean(value);
  }
  
  public static boolean isThemisEnableTable(TableDescriptor desc) {
    for (ColumnFamilyDescriptor columnDesc : desc.getColumnFamilies()) {
      if (isThemisEnableFamily(columnDesc)) {
        return true;
      }
    }
    return false;
  }
  
  protected synchronized void startExpiredTimestampCalculator(MasterServices masterServices, Configuration configuration)
      throws IOException {
    if (themisExpiredTsCalculator != null) {
      return;
    }

    LOG.info("try to start thread to compute the expired timestamp for themis table");
    this.zk = masterServices.getZooKeeper();
    themisExpiredTsZNodePath = getThemisExpiredTsZNodePath(this.zk);

    connection = ConnectionFactory.createConnection(configuration);
    lockCleaner = new ServerLockCleaner(connection, new ThemisCoprocessorClient(connection));
    
    String expiredTsCalculatePeriodStr = configuration
        .get(THEMIS_EXPIRED_TIMESTAMP_CALCULATE_PERIOD_KEY);
    if (expiredTsCalculatePeriodStr == null) {
      expiredTsCalculatePeriod = TransactionTTL.readTransactionTTL / 1000;
    } else {
      expiredTsCalculatePeriod = Integer.parseInt(expiredTsCalculatePeriodStr);
    }
    
    themisExpiredTsCalculator = new ExpiredTimestampCalculator(CHORE_THREAD_NAME, expiredTsCalculatePeriod * 1000);
    Threads.setDaemonThreadRunning(themisExpiredTsCalculator);
    LOG.info("successfully start thread to compute the expired timestamp for themis table, timestampType="
        + TransactionTTL.timestampType + ", calculatorPeriod=" + expiredTsCalculatePeriod + ", zkPath="
        + themisExpiredTsZNodePath);
  }
  
  class ExpiredTimestampCalculator extends Thread {
    private long currentExpiredTs;

    public ExpiredTimestampCalculator(String name, long currentExpiredTs) {
      super(name);
      this.currentExpiredTs = currentExpiredTs;
    }
    
    @Override
    public void run() {
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
  
  public static List<String> getThemisTables(Connection connection) throws IOException {
    List<String> tableNames = Lists.newArrayList();
    try (Admin admin = connection.getAdmin()) {
      List<TableDescriptor> tables = admin.listTableDescriptors();
      for (TableDescriptor table : tables) {
        if (isThemisEnableTable(table)) {
          tableNames.add(table.getTableName().getNameAsString());
        }
      }
    } catch (IOException e) {
      LOG.error("get table names fail", e);
      throw e;
    }
    return tableNames;
  }
  
  public static String getThemisExpiredTsZNodePath(ZKWatcher zk) {
    return zk.getZNodePaths().baseZNode + "/" + THEMIS_EXPIRED_TIMESTAMP_ZNODE_NAME;
  }
  
  public static long getThemisExpiredTsFromZk(ZKWatcher zk) throws Exception {
    return getThemisExpiredTsFromZk(zk, getThemisExpiredTsZNodePath(zk));
  }
  
  public static long getThemisExpiredTsFromZk(ZKWatcher zk, String path) throws Exception {
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
      Table hTable = null;
      int cleanedLockCount = 0;
      try {
        hTable = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(ColumnUtil.LOCK_FAMILY_NAME);
        scan.setTimeRange(0, ts);
        ResultScanner scanner = hTable.getScanner(scan);
        Result result;
        while ((result = scanner.next()) != null) {
          for (Cell kv : result.listCells()) {
            ThemisLock lock = ThemisLock.parseFromByte(kv.getValueArray());
            Column dataColumn = ColumnUtil.getDataColumnFromConstructedQualifier(new Column(kv.getFamilyArray(),
                kv.getQualifierArray()));
            lock.setColumn(new ColumnCoordinate(tableNameBytes, kv.getRowArray(),
                dataColumn.getFamily(), dataColumn.getQualifier()));
            lockCleaner.cleanLock(lock);
            ++cleanedLockCount;
            LOG.info("themis clean expired lock, lockTs=" + kv.getTimestamp() + ", expiredTs=" + ts
                + ", lock=" + ThemisLock.parseFromByte(kv.getValueArray()));
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