package com.xiaomi.infra.themis.validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.collect.Lists;
import com.xiaomi.infra.themis.IncrementalTimestamp;
import com.xiaomi.infra.themis.ReadOnlyTransaction;
import com.xiaomi.infra.themis.WorkerManager;
import com.xiaomi.infra.themis.checker.ValueChecker;
import com.xiaomi.infra.themis.checker.WriteConflictChecker;

public class Validator {
  private static final Log LOG = LogFactory.getLog(Validator.class);
  public static HConnection connection;
  public static int tableCount = 2;
  public static byte[][] tableNames = new byte[tableCount][];
  public static int rowCount = 20;
  public static int cfCount = 2;
  public static byte[][] families = null;
  public static int qfCount = 2;
  public static byte[][] qualifiers = null;
  public static int maxCellCountOfTransaction = 5;
  public static int wokerCount = 1;
  public static int regionCount = 20;
  public static List<ColumnCoordinate> columnCoordinates;
  public static List<Integer> values;
  public static long totalValue;
  public static boolean enableTotalValueChecker;
  public static boolean enableWriteConflictChecker;
  public static ValueChecker valueChecker;
  public static WriteConflictChecker writeConflictChecker;
  public static String clusterUri;
  public static boolean initValueUsingTransaction;
  public static boolean usingThreadPool;
  
  public static void initValidator(Configuration conf) throws IOException {
    if (clusterUri == null) {
      connection = HConnectionManager.createConnection(conf);
    } else {
      connection = HConnectionManager.createConnection(conf, clusterUri);
    }
    generateColumns();
    generateColumnValues();
    if (initValueUsingTransaction) {
      initColumnValues();
      long readTotalValue = ValueChecker.getTotalValue(new ReadOnlyTransaction(connection));
      if (readTotalValue != totalValue) {
        LOG.fatal("total value init check mismatch, expect=" + totalValue + ", actual=" + readTotalValue);
        System.exit(-1);
      } else {
        LOG.warn("total value after init matched, totalvalue=" + totalValue);
      }
    } else {
      initColumnValuesWithoutTransaction();
    }
  }
  
  protected static byte[] generateRowKey(int index) {
    String indexStr = "" + index;
    for (int i = indexStr.length() ; i < 2; ++i) {
      indexStr = "0" + indexStr;
    }
    return Bytes.toBytes(ValidatorConstant.ROW_PREFIX + indexStr);
  }
  
  public static void generateColumns() {
    columnCoordinates = new ArrayList<ColumnCoordinate>();
    for (int i = 0; i < tableCount; ++i) {
      byte[] tableName = Bytes.toBytes(ValidatorConstant.TABLE_NAME_PREFIX + i);
      tableNames[i] = tableName;
      for (int j = 0; j < rowCount; ++j) {
        byte[] rowkey = generateRowKey(j);
        for (int k = 0; k < cfCount; ++k) {
          byte[] cf = Bytes.toBytes(ValidatorConstant.FAMILY_PREFIX + k);
          for (int l = 0; l < qfCount; ++l) {
            byte[] qualifier = Bytes.toBytes(ValidatorConstant.QUALIFIER_PREFIX + l);
            ColumnCoordinate columnCoordinate = new ColumnCoordinate(tableName, rowkey, cf, qualifier);
            columnCoordinates.add(columnCoordinate);
          }
        }
      }
    }
    families = new byte[cfCount][];
    for (int i = 0; i < cfCount; ++i) {
      byte[] cf = Bytes.toBytes(ValidatorConstant.FAMILY_PREFIX + i);
      families[i] = cf;
    }
    qualifiers = new byte[qfCount][];
    for (int i = 0; i < qfCount; ++i) {
      byte[] qualifier = Bytes.toBytes(ValidatorConstant.QUALIFIER_PREFIX + i);
      qualifiers[i] = qualifier;
    }
    Collections.shuffle(columnCoordinates);
    LOG.info("generated columns, columnCount=" + columnCoordinates.size());
  }
  
  protected static void generateColumnValues() {
    Random rd = new Random();
    totalValue = 0;
    values = new ArrayList<Integer>();
    for (int i = 0; i < columnCoordinates.size(); ++i) {
      int value = rd.nextInt(ValidatorConstant.MAX_COLUMN_INIT_VALUE) + 1 ;
      values.add(value);
      totalValue += values.get(i);
    }
    LOG.info("generated values for columns, Totalvalue=" + totalValue);
  }
  
  protected static void initColumnValues() throws IOException {
    createTables();
    Transaction transaction = new Transaction(connection.getConfiguration(), connection);
    for (int i = 0; i < columnCoordinates.size(); ++i) {
      ColumnCoordinate columnCoordinate = columnCoordinates.get(i);
      ThemisPut put = new ThemisPut(columnCoordinate.getRow()).add(columnCoordinate.getFamily(),
        columnCoordinate.getQualifier(), Bytes.toBytes(values.get(i)));
      transaction.put(columnCoordinate.getTableName(), put);
    }
    transaction.commit();
    LOG.info("transaction commit success to init values");
  }
  
  // for efficiency consideration
  protected static void initColumnValuesWithoutTransaction() throws IOException {
    createTables();
    long prewriteTs = new IncrementalTimestamp(connection.getConfiguration()).getRequestIdWithTimestamp().getSecond();
    long commitTs = new IncrementalTimestamp(connection.getConfiguration()).getRequestIdWithTimestamp().getSecond();
    for (int i = 0; i < columnCoordinates.size(); ++i) {
      ColumnCoordinate columnCoordinate = columnCoordinates.get(i);
      HTableInterface table = connection.getTable(columnCoordinate.getTableName());
      Put dataPut = new Put(columnCoordinate.getRow()).add(columnCoordinate.getFamily(), columnCoordinate.getQualifier(),
        prewriteTs, Bytes.toBytes(values.get(i)));
      Column wc = ColumnUtil.getPutColumn(columnCoordinate);
      Put writePut = new Put(columnCoordinate.getRow()).add(wc.getFamily(), wc.getQualifier(), commitTs, Bytes.toBytes(prewriteTs));
      table.put(Lists.newArrayList(dataPut, writePut));
      table.close();
    }
    LOG.info("put success to init values");
  }
  
  protected static byte[][] generateRegionSplitKeys() {
    byte[][] splitKeys = new byte[regionCount][];
    for (int i = 0; i < splitKeys.length; ++i) {
      splitKeys[i] = generateRowKey(i);
    }
    return splitKeys;
  }
  
  protected static void createTables() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(connection.getConfiguration());
    for (int i = 0; i < tableCount; ++i) {
      String tableName = ValidatorConstant.TABLE_NAME_PREFIX + i;
      try {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      } catch (TableNotFoundException e) {
      } catch (TableNotEnabledException e) {
        admin.deleteTable(tableName);
      }
      
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      for (int j = 0; j < cfCount; ++j) {
        String cf = ValidatorConstant.FAMILY_PREFIX + j;
        HColumnDescriptor columnDesc = new HColumnDescriptor(cf);
        columnDesc.setMaxVersions(Integer.MAX_VALUE);
        tableDesc.addFamily(columnDesc);
      }
      HColumnDescriptor lockFamily = new HColumnDescriptor(ColumnUtil.LOCK_FAMILY_NAME);
      lockFamily.setMaxVersions(1);
      lockFamily.setInMemory(true);
      tableDesc.addFamily(lockFamily);
      
      if (regionCount == 1) {
        admin.createTable(tableDesc);
      } else {
        admin.createTable(tableDesc, generateRegionSplitKeys()); 
      }
    }
    admin.close();
    LOG.info("create table finish for Validator");
  }
  
  public static void startValidator(Configuration conf) throws Exception {
    initValidator(conf);
    // start writers
    WriteWorker[] writers = new WriteWorker[wokerCount];
    for (int i = 0; i < writers.length; ++i) {
      writers[i] = new WriteWorker(conf);
      writers[i].start();
    }
    LOG.info("WriteWorker started, writer counts=" + writers.length);
    startCheckers(conf);
  }
  
  protected static void startCheckers(Configuration conf) throws IOException {
    // start total value checker and write conflict checker
    if (enableTotalValueChecker) {
      valueChecker = new ValueChecker(conf);
      valueChecker.start();
      LOG.info("TotalValueCheckWorker started");
    }
    if (enableWriteConflictChecker) {
      writeConflictChecker = new WriteConflictChecker(conf);
      writeConflictChecker.start();
      LOG.info("WriteConflictChecker started");
    }
  }
  
  protected static void setThemisConfig(Configuration conf) throws IOException {
    if (conf.get(TransactionConstant.TIMESTAMP_ORACLE_CLASS_KEY) == null) {
      conf.set(TransactionConstant.TIMESTAMP_ORACLE_CLASS_KEY, IncrementalTimestamp.class.getName());
    }
    conf.set(TransactionConstant.WORKER_REGISTER_CLASS_KEY, WorkerManager.class.getName());
  }
  
  protected static void setOptionsFromConfig(Configuration conf) throws IOException {
    tableCount = conf.getInt(ValidatorConstant.VALIDATOR_TABLE_COUNT_KEY, 2);
    rowCount = conf.getInt(ValidatorConstant.VALIDATOR_ROW_COUNT_KEY, 20);
    cfCount = conf.getInt(ValidatorConstant.VALIDATOR_CF_COUNT_KEY, 2);
    qfCount = conf.getInt(ValidatorConstant.VALIDATOR_QF_COUNT_KEY, 2);
    maxCellCountOfTransaction = conf.getInt(ValidatorConstant.VALIDATOR_TRANSACTION_MAX_CELL_COUNT_KEY, 5);
    regionCount = conf.getInt(ValidatorConstant.VALIDATOR_TABLE_REGION_COUNT, 1);
    wokerCount = conf.getInt(ValidatorConstant.VALIDATOR_WORKER_COUNT_KEY, 10);
    enableTotalValueChecker = conf.getBoolean(ValidatorConstant.VALIDATOR_ENABLE_TOTAL_VALUE_CHECKER, true);
    enableWriteConflictChecker = conf.getBoolean(ValidatorConstant.VALIDATOR_ENABLE_WRITE_CONFLCIT_CHECKER, true);
    initValueUsingTransaction = conf.getBoolean(ValidatorConstant.VALIDATOR_INIT_USING_TRANSACTION, true);
    usingThreadPool = conf.getBoolean(TransactionConstant.THEMIS_ENABLE_CONCURRENT_RPC, false);
    LOG.warn("Validator config:");
    for (int i = 0; i < ValidatorConstant.configKey.length; ++i) {
      LOG.warn(ValidatorConstant.configKey[i] + "=" + conf.get(ValidatorConstant.configKey[i]));
    }
  }
  
  public static Configuration initConfig() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    if (conf.get("hbase.cluster.name") == null) {
      conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
      conf.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.WritableRpcEngine");
    } else {
      clusterUri = "hbase://" + conf.get("hbase.cluster.name");
    }
    setThemisConfig(conf);
    setOptionsFromConfig(conf);
    return conf;
  }
  
  public static void waitToExit() throws Exception {
    boolean checkerEnable = false;
    if (valueChecker != null) {
      checkerEnable = true;
      valueChecker.join();
    }
    System.err.println("value checker exit");
    if (writeConflictChecker != null) {
      checkerEnable = true;
      writeConflictChecker.join();
    }
    System.err.println("writeconflict checker exit");
    if (!checkerEnable) {
      Threads.sleep(Long.MAX_VALUE);
    }
    System.out.println("waitToExit exit");
  }
  
  public static HConnection createHConnection(Configuration conf) throws IOException {
    if (clusterUri == null) {
      return HConnectionManager.createConnection(conf);
    } else {
      return HConnectionManager.createConnection(conf, clusterUri);
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = initConfig();
    startValidator(conf);
    waitToExit();
  }
}