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
  public static final byte[] MAIN_TABLE = Bytes.toBytes("test_index_main");
  public static final byte[] INDEX_TABLE = Bytes.toBytes("__themis_index_test_index_main_ThemisCF_Qualifier_test_index");
  public static final byte[] INDEX_FAMILY = FAMILY;
  public static final byte[] INDEX_QUALIFIER = QUALIFIER;
  public static final byte[] IDNEX_NAME = Bytes.toBytes("test_index:Qualifier");
  public static final IndexColumn INDEX_COLUMN = new IndexColumn(MAIN_TABLE, FAMILY, QUALIFIER);
  
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
    HTableDescriptor tableDesc = new HTableDescriptor(MAIN_TABLE);
    HColumnDescriptor columnDesc = new HColumnDescriptor(INDEX_FAMILY);
    columnDesc.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString());
    columnDesc.setValue(Bytes.toBytes(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY),
      IDNEX_NAME);
    tableDesc.addFamily(columnDesc);
    columnDesc = new HColumnDescriptor(ANOTHER_FAMILY);
    columnDesc.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString());
    tableDesc.addFamily(columnDesc);
    admin.createTable(tableDesc);
  }
  
  protected void deleteTableForIndexTest() throws IOException {
    admin.disableTable(MAIN_TABLE);
    admin.deleteTable(MAIN_TABLE); 
  }
}
