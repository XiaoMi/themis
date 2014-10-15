package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.themis.TransactionConstant;
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
  
  protected HTableInterface mainTable = null;
  protected HBaseAdmin admin = null;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    useMiniCluster();
    conf.set(TransactionConstant.INDEXER_CLASS_KEY, DefaultIndexer.class.getName());
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    startMiniCluster(conf);
    createTableForIndexTest();
  }
  
  @Before
  public void initEnv() throws IOException {
    super.initEnv();
    mainTable = connection.getTable(MAIN_TABLE);
    deleteOldDataAndUpdateTs(mainTable);
    admin = new HBaseAdmin(connection);
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
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(conf);
      if (admin.tableExists(tableName)) {
        return;
      }
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      HColumnDescriptor columnDesc = new HColumnDescriptor(INDEX_FAMILY);
      columnDesc.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString());
      columnDesc.setValue(
        Bytes.toBytes(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY), IDNEX_NAME);
      tableDesc.addFamily(columnDesc);
      columnDesc = new HColumnDescriptor(ANOTHER_FAMILY);
      columnDesc.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString());
      tableDesc.addFamily(columnDesc);
      admin.createTable(tableDesc);
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
  
  protected static void deleteTableForIndexTest(byte[] tableName) throws IOException {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(conf);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
}
