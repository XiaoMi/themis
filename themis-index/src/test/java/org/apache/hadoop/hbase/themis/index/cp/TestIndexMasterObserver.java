package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class TestIndexMasterObserver extends TransactionTestBase {
  @Test
  public void testCheckIndexNames() throws IOException {
    HColumnDescriptor desc = new HColumnDescriptor("C");
    String familyIndexAttribute = "index_a:a;index_a:a";
    
    desc.setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY, familyIndexAttribute);
    try {
      IndexMasterObserver.checkIndexNames(desc);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("duplicate secondary index definition") >= 0);
    }
    
    String[] familyIndexAttributes = new String[] {"index_a:", ":index_a", ":"};
    for (String indexAttribute : familyIndexAttributes) {
      desc.setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY,
        indexAttribute);
      try {
        IndexMasterObserver.checkIndexNames(desc);
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().indexOf("illegal secondary index definition") >= 0);
      }
    }
    
    familyIndexAttributes = new String[]{"index_a:a", "index_a:a;index_a:b;index_b:a"};
    for (String indexAttribute : familyIndexAttributes) {
      desc.setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY, indexAttribute);
      IndexMasterObserver.checkIndexNames(desc);
    }
  }
  
  @Test
  public void testConstructSecondaryIndexTableName() {
    Assert.assertEquals("__themis_index_a_b_c_d",
      IndexMasterObserver.constructSecondaryIndexTableName("a", "b", "c", "d"));
  }
  
  @Test
  public void testGetSecondaryIndexTableDesc() throws IOException {
    String indexTableName = "__themis_index_a_b_c_d";
    HTableDescriptor desc = IndexMasterObserver.getSecondaryIndexTableDesc(indexTableName);
    Assert.assertNotNull(desc.getValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_ATTRIBUTE_KEY));
    HColumnDescriptor family = desc.getFamily(Bytes
        .toBytes(IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY));
    Assert.assertNotNull(family);
    Assert.assertNotNull(family.getValue(ThemisMasterObserver.THEMIS_ENABLE_KEY));
  }
  
  @Test
  public void testCreateDeleteTableWithSecondaryIndex() throws IOException {
    // create normal table with secondary index table prefix
    HBaseAdmin admin = new HBaseAdmin(conf);
    String tableName = IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX + "_a";
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    try {
      admin.createTable(tableDesc);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("is preserved") >= 0);
    }
    
    // create themis table without index enable
    tableName = "test_table";
    tableDesc = new HTableDescriptor(tableName);
    tableDesc.addFamily(IndexMasterObserver.getSecondaryIndexFamily());
    admin.createTable(tableDesc);
    HTableDescriptor[] tableDescs = admin.listTables();
    for (HTableDescriptor desc : tableDescs) {
      Assert.assertFalse(desc.getNameAsString().indexOf(
        IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX) >= 0);
    }
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    
    // create table with index attribute but without themis enable key
    tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor columnDesc = new HColumnDescriptor("C");
    columnDesc.setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY,
      Boolean.TRUE.toString());
    tableDesc.addFamily(columnDesc);
    try {
      admin.createTable(tableDesc);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("must be set on themis-enabled family") >= 0);
    }
    
    // create themis table with secondary index attribute
    tableDesc = new HTableDescriptor(tableName);
    byte[] family = Bytes.toBytes("C");
    columnDesc = new HColumnDescriptor(family);
    columnDesc.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString());
    String indexAttribute = "test_index:c";
    columnDesc.setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY,
      indexAttribute);
    tableDesc.addFamily(columnDesc);
    admin.createTable(tableDesc);
    tableDescs = admin.listTables();
    boolean containMainTable = false;
    boolean containIndexTable = false;
    for (HTableDescriptor desc : tableDescs) {
      if (desc.getNameAsString().equals(tableName)) {
        containMainTable = true;
        Assert.assertNotNull(desc.getFamily(ColumnUtil.LOCK_FAMILY_NAME));
      } else if (desc.getNameAsString().equals("__themis_index_test_table_C_c_test_index")) {
        containIndexTable = true;
        Assert.assertNotNull(desc.getFamily(Bytes
            .toBytes(IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY)));
        Assert.assertNotNull(desc.getFamily(ColumnUtil.LOCK_FAMILY_NAME));
      }
    }
    Assert.assertTrue(containMainTable);
    Assert.assertTrue(containIndexTable);
    
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    tableDescs = admin.listTables();
    for (HTableDescriptor desc : tableDescs) {
      if (desc.getNameAsString().equals(tableName)) {
        Assert.fail("fail to delete table:" + tableName);
      } else if (desc.getNameAsString().equals("__themis_index_test_table_C_c_test_index")) {
        Assert.fail("fail to delete table:__themis_index_test_table_C_c_test_index");
      }
    }
    admin.close();
  }
}