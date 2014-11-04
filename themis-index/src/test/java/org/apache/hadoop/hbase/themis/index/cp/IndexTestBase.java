package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;

public class IndexTestBase extends ClientTestBase {
  public static final byte[] INDEX_TEST_MAIN_TABLE_NAME = Bytes.toBytes("test_index_main");
  public static final byte[] INDEX_TEST_INDEX_TABLE_NAME = Bytes.toBytes("__themis_index_test_index_main_C_c_test_index");
  public static final byte[] INDEX_TEST_MAIN_TABLE_FAMILY_NAME = Bytes.toBytes("C");
  public static final byte[] INDEX_TEST_MAIN_TABLE_QUALIFIER_NAME = Bytes.toBytes("c");
  public static final byte[] TEST_IDNEX_NAME = Bytes.toBytes("test_index:c");
  
  protected HBaseAdmin admin = null;
  
  @Before
  public void initEnv() throws IOException {
    super.initEnv();
    admin = new HBaseAdmin(connection);
  }
  
  @After
  public void tearUp() throws IOException {
    super.tearUp();
    if (admin != null) {
      admin.close();
    }
  }
  
  protected void createTableForIndexTest() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(INDEX_TEST_MAIN_TABLE_NAME);
    byte[] family = INDEX_TEST_MAIN_TABLE_FAMILY_NAME;
    HColumnDescriptor columnDesc = new HColumnDescriptor(family);
    columnDesc.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString());
    columnDesc.setValue(Bytes.toBytes(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY),
      TEST_IDNEX_NAME);
    tableDesc.addFamily(columnDesc);
    admin.createTable(tableDesc);
  }
  
  protected void deleteTableForIndexTest() throws IOException {
    admin.disableTable(INDEX_TEST_MAIN_TABLE_NAME);
    admin.deleteTable(INDEX_TEST_MAIN_TABLE_NAME); 
  }
}
