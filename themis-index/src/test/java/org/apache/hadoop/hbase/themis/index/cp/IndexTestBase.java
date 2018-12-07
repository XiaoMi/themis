package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
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

import com.xiaomi.infra.thirdparty.com.google.common.io.Closeables;

public class IndexTestBase extends ClientTestBase {
  public static final TableName MAIN_TABLE = TableName.valueOf("test_index_main");
  public static final TableName INDEX_TABLE =
    TableName.valueOf("__themis_index_test_index_main_ThemisCF_Qualifier_test_index");
  public static final byte[] INDEX_FAMILY = FAMILY;
  public static final byte[] INDEX_QUALIFIER = QUALIFIER;
  public static final byte[] INDEX_NAME = Bytes.toBytes("test_index:Qualifier");
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
    mainTable = connection.getTable(MAIN_TABLE);
    deleteOldDataAndUpdateTs(mainTable);
    admin = connection.getAdmin();
  }

  @After
  public void tearUp() throws IOException {
    super.tearUp();
    Closeables.close(mainTable, true);
    Closeables.close(admin, true);
  }

  protected static void createTableForIndexTest() throws IOException {
    createTableForIndexTest(MAIN_TABLE);
  }

  protected static void createTableForIndexTest(TableName tableName) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(conf);
      Admin admin = conn.getAdmin()) {
      if (admin.tableExists(tableName)) {
        return;
      }
      TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(INDEX_FAMILY)
          .setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString())
          .setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY_BYTES,
            INDEX_NAME)
          .build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(ANOTHER_FAMILY)
          .setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString()).build())
        .build();
      admin.createTable(tableDesc);
    }
  }

  protected static void deleteTableForIndexTest(TableName tableName) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(conf);
      Admin admin = conn.getAdmin()) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }
}
