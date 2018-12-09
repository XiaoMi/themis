package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
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
import org.apache.hadoop.hbase.themis.cp.ThemisCpStatistics;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpointClient;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.xiaomi.infra.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@CoreCoprocessor
public class ThemisMasterObserver implements MasterObserver, MasterCoprocessor {

  private static final Logger LOG = LoggerFactory.getLogger(ThemisMasterObserver.class);

  public static final String THEMIS_ENABLE_KEY = "THEMIS_ENABLE";
  public static final byte[] THEMIS_ENABLE_KEY_BYTES = Bytes.toBytes(THEMIS_ENABLE_KEY);

  public static final String THEMIS_EXPIRED_TIMESTAMP_CALCULATE_PERIOD_KEY =
    "themis.expired.timestamp.calculator.period";
  public static final String THEMIS_EXPIRED_DATA_CLEAN_ENABLE_KEY =
    "themis.expired.data.clean.enable";
  public static final String THEMIS_EXPIRED_TIMESTAMP_ZNODE_NAME = "themis-expired-ts";

  @VisibleForTesting
  MasterServices services;

  @VisibleForTesting
  String themisExpiredTsZNodePath;

  @VisibleForTesting
  ServerLockCleaner lockCleaner;

  private ScheduledExecutorService executor;

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.ofNullable(this);
  }

  @Override
  public void start(@SuppressWarnings("rawtypes") CoprocessorEnvironment ctx) throws IOException {
    assert ctx instanceof HasMasterServices;
    services = ((HasMasterServices) ctx).getMasterServices();
    ColumnUtil.init(ctx.getConfiguration());
    if (ctx.getConfiguration().getBoolean(THEMIS_EXPIRED_DATA_CLEAN_ENABLE_KEY, true)) {
      TransactionTTL.init(ctx.getConfiguration());
      ThemisCpStatistics.init(ctx.getConfiguration());
      startExpiredTimestampCalculator();
    }
  }

  @Override
  public void stop(@SuppressWarnings("rawtypes") CoprocessorEnvironment ctx) throws IOException {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Override
  public TableDescriptor preCreateTableRegionsInfos(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc) throws IOException {
    if (!isThemisEnableTable(desc)) {
      return desc;
    }
    return addAuxiliaryFamily(desc);
  }

  @Override
  public TableDescriptor preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
      throws IOException {
    // TODO: now we only support modify table call from libsds, an alter table from shell will fail.
    // But we can know that if this is an alter table from shell, by comparing the currentDescriptor
    // and the newDescriptor, and find out how to deal with it. Can be done later.
    if (!isThemisEnableTable(newDescriptor)) {
      return newDescriptor;
    }
    return addAuxiliaryFamily(newDescriptor);
  }

  private TableDescriptor addAuxiliaryFamily(TableDescriptor desc) throws DoNotRetryIOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(desc);
    int replicationScope = ColumnFamilyDescriptorBuilder.DEFAULT_REPLICATION_SCOPE;
    for (ColumnFamilyDescriptor colDesc : desc.getColumnFamilies()) {
      checkColumnDescriptorWhenThemisEnable(colDesc);
      if (isThemisEnableFamily(colDesc)) {
        builder.modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(colDesc)
          .setMaxVersions(Integer.MAX_VALUE).build());
        // FIXME(qiankai): just use the scope value of the last CF?
        int scope = colDesc.getScope();
        if (scope != ColumnFamilyDescriptorBuilder.DEFAULT_REPLICATION_SCOPE) {
          replicationScope = scope;
        }
      }
    }
    builder.setColumnFamily(createLockFamily(replicationScope));
    LOG.info(
      "add family '" + ColumnUtil.LOCK_FAMILY_NAME_STRING + "' for table:" + desc.getTableName());
    if (!ColumnUtil.isCommitToSameFamily()) {
      addCommitFamilies(builder, replicationScope);
      LOG.info("add commit family '" + ColumnUtil.PUT_FAMILY_NAME + "' and '" +
        ColumnUtil.DELETE_FAMILY_NAME + "' for table:" + desc.getTableName());
    }
    return builder.build();
  }

  private void checkColumnDescriptorWhenThemisEnable(ColumnFamilyDescriptor colDesc)
      throws DoNotRetryIOException {
    if (Bytes.equals(ColumnUtil.LOCK_FAMILY_NAME, colDesc.getName())) {
      throw new DoNotRetryIOException(
        "family '" + ColumnUtil.LOCK_FAMILY_NAME_STRING + "' is preserved by themis when " +
          THEMIS_ENABLE_KEY + " is true, please change your family name");
    }

    if (isThemisEnableFamily(colDesc)) {
      // make sure TTL and MaxVersion is not set by user
      if (colDesc.getTimeToLive() != HConstants.FOREVER) {
        throw new DoNotRetryIOException("can not set TTL for family '" + colDesc.getNameAsString() +
          "' when " + THEMIS_ENABLE_KEY + " is true, TTL=" + colDesc.getTimeToLive());
      }
      if (colDesc.getMaxVersions() != ColumnFamilyDescriptorBuilder.DEFAULT_MAX_VERSIONS &&
        colDesc.getMaxVersions() != Integer.MAX_VALUE) {
        throw new DoNotRetryIOException(
          "can not set MaxVersion for family '" + colDesc.getNameAsString() + "' when " +
            THEMIS_ENABLE_KEY + " is true, MaxVersion=" + colDesc.getMaxVersions());
      }
    }
  }

  @VisibleForTesting
  static ColumnFamilyDescriptor createLockFamily(int scope) {
    // TODO(cuijianwei) : choose the best bloom filter type
    return ColumnFamilyDescriptorBuilder.newBuilder(ColumnUtil.LOCK_FAMILY_NAME).setInMemory(true)
      .setMaxVersions(1).setTimeToLive(HConstants.FOREVER).setScope(scope).build();
  }

  private static void addCommitFamilies(TableDescriptorBuilder builder, int scope) {
    for (byte[] family : ColumnUtil.COMMIT_FAMILY_NAME_BYTES) {
      builder.setColumnFamily(getCommitFamily(family, scope));
    }
  }

  @VisibleForTesting
  static ColumnFamilyDescriptor getCommitFamily(byte[] familyName, int scope) {
    return ColumnFamilyDescriptorBuilder.newBuilder(familyName).setTimeToLive(HConstants.FOREVER)
      .setMaxVersions(Integer.MAX_VALUE).setScope(scope).build();
  }

  public static boolean isThemisEnableFamily(ColumnFamilyDescriptor desc) {
    byte[] value = desc.getValue(THEMIS_ENABLE_KEY_BYTES);
    return value != null && Boolean.parseBoolean(Bytes.toString(value));
  }

  public static boolean isThemisEnableTable(TableDescriptor desc) {
    return Stream.of(desc.getColumnFamilies()).anyMatch(ThemisMasterObserver::isThemisEnableFamily);
  }

  private synchronized void startExpiredTimestampCalculator() throws IOException {
    if (executor != null) {
      return;
    }
    LOG.info("try to start thread to compute the expired timestamp for themis table");
    themisExpiredTsZNodePath = getThemisExpiredTsZNodePath(services.getZooKeeper());
    lockCleaner = new ServerLockCleaner(services.getConnection(),
      new ThemisEndpointClient(services.getConnection()));

    String expiredTsCalculatePeriodStr =
      services.getConfiguration().get(THEMIS_EXPIRED_TIMESTAMP_CALCULATE_PERIOD_KEY);
    long expiredTsCalculatePeriod;
    if (expiredTsCalculatePeriodStr == null) {
      expiredTsCalculatePeriod = TransactionTTL.readTransactionTTL / 1000;
    } else {
      expiredTsCalculatePeriod = Integer.parseInt(expiredTsCalculatePeriodStr);
    }

    executor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
      .setNameFormat("Themis-Expired-Ts-Calculator").setDaemon(true).build());
    executor.scheduleAtFixedRate(this::calculateExpiredTimestamp, expiredTsCalculatePeriod,
      expiredTsCalculatePeriod, TimeUnit.SECONDS);
    LOG.info(
      "successfully start thread to compute the expired timestamp for themis table, timestampType=" +
        TransactionTTL.timestampType + ", calculatorPeriod=" + expiredTsCalculatePeriod +
        ", zkPath=" + themisExpiredTsZNodePath);
  }

  private void calculateExpiredTimestamp() {
    long currentMs = System.currentTimeMillis();
    long currentExpiredTs = TransactionTTL.getExpiredTimestampForReadByDataColumn(currentMs);
    LOG.info("computed expired timestamp for themis, currentExpiredTs=" + currentExpiredTs +
      ", currentMs=" + currentMs);
    try {
      cleanLockBeforeTimestamp(currentExpiredTs);
      setExpiredTsToZk(currentExpiredTs);
    } catch (Exception e) {
      LOG.error("themis clean expired lock fail", e);
    }
  }

  @VisibleForTesting
  static List<TableName> getThemisTables(Connection conn) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      return admin.listTableDescriptors().stream().filter(ThemisMasterObserver::isThemisEnableTable)
        .map(desc -> desc.getTableName()).collect(Collectors.toList());
    }
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
    ZKUtil.createSetData(services.getZooKeeper(), themisExpiredTsZNodePath,
      Bytes.toBytes(String.valueOf(currentExpiredTs)));
    LOG.info("successfully set currentExpiredTs to zk, currentExpiredTs=" + currentExpiredTs +
      ", zkPath=" + themisExpiredTsZNodePath);
  }

  public void cleanLockBeforeTimestamp(long ts) throws IOException {
    for (TableName tableName : getThemisTables(services.getConnection())) {
      LOG.info("start to clean expired lock for themis table:" + tableName);
      int cleanedLockCount = 0;
      Scan scan = new Scan().addFamily(ColumnUtil.LOCK_FAMILY_NAME).setTimeRange(0, ts);
      try (Table table = services.getConnection().getTable(tableName);
          ResultScanner scanner = table.getScanner(scan)) {
        Result result = null;
        while ((result = scanner.next()) != null) {
          for (Cell cell : result.rawCells()) {
            ThemisLock lock = ThemisLock.parseFromBytes(cell.getValueArray(), cell.getValueOffset(),
              cell.getValueLength());
            Column dataColumn = ColumnUtil.getDataColumnFromConstructedQualifier(
              new Column(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell)));
            lock.setColumn(new ColumnCoordinate(tableName, CellUtil.cloneRow(cell),
              dataColumn.getFamily(), dataColumn.getQualifier()));
            lockCleaner.cleanLock(lock);
            ++cleanedLockCount;
            LOG.info("themis clean expired lock, lockTs=" + cell.getTimestamp() + ", expiredTs=" +
              ts + ", lock=" + lock);
          }
        }
      }
      LOG.info("finish clean expired lock for themis table:" + tableName + ", cleanedLockCount=" +
        cleanedLockCount);
    }
  }
}
