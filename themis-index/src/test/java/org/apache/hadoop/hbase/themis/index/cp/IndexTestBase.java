package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL.TimestampType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public class IndexTestBase extends ClientTestBase {
  public static final byte[] MAIN_TABLE = Bytes.toBytes("test_index_main");
  public static final byte[] INDEX_TABLE = Bytes.toBytes("__themis_index_test_index_main_ThemisCF_Qualifier_test_index");
  public static final byte[] INDEX_FAMILY = FAMILY;
  public static final byte[] INDEX_QUALIFIER = QUALIFIER;
  public static final byte[] IDNEX_NAME = Bytes.toBytes("test_index:Qualifier");
  public static final IndexColumn INDEX_COLUMN = new IndexColumn(MAIN_TABLE, FAMILY, QUALIFIER);
  
  protected Table mainTable = null;
  protected Admin admin = null;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    if (useMiniCluster) {
      useMiniCluster();
      TransactionTTL.timestampType = TimestampType.MS;
      conf.set(TransactionConstant.INDEXER_CLASS_KEY, DefaultIndexer.class.getName());
      conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
      startMiniCluster(conf);
    }
    createTableForIndexTest();
  }
  
  @Before
  public void initEnv() throws IOException {
    super.initEnv();
    mainTable = connection.getTable(TableName.valueOf(MAIN_TABLE));
    deleteOldDataAndUpdateTs(mainTable);
    admin = connection.getAdmin();
  }
  
  @After
  public void tearUp() throws IOException {
    super.tearUp();
    if (mainTable != null) {
      mainTable.close();
    }
    if (admin != null) {
      admin.close();
    }
  }
  
  protected static void createTableForIndexTest() throws IOException {
    createTableForIndexTest(MAIN_TABLE);
  }
  
  protected static void createTableForIndexTest(byte[] tableName) throws IOException {
    Admin admin = null;
    TableName tn = TableName.valueOf(tableName);
    try {
      admin = ConnectionFactory.createConnection(conf).getAdmin();
      if (admin.tableExists(tn)) {
        return;
      }

      ColumnFamilyDescriptor columnDesc = ColumnFamilyDescriptorBuilder
              .newBuilder(INDEX_FAMILY)
              .setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString())
              .setValue(Bytes.toBytes(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY), IDNEX_NAME)
              .build();

      ColumnFamilyDescriptor anotherColumnDesc = ColumnFamilyDescriptorBuilder
              .newBuilder(ANOTHER_FAMILY)
              .setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString())
              .build();
      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tn)
              .setColumnFamily(columnDesc)
              .setColumnFamily(anotherColumnDesc)
              .build();
      admin.createTable(tableDescriptor);
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
  
  protected static void deleteTableForIndexTest(byte[] tableName) throws IOException {
    Admin admin = null;
    TableName tn = TableName.valueOf(tableName);
    try {
      admin = ConnectionFactory.createConnection(conf).getAdmin();
      admin.disableTable(tn);
      admin.deleteTable(tn);
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
}
