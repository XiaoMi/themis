package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class TestIndexMasterObserver extends IndexTestBase {
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
    HTableDescriptor tableDesc = new HTableDescriptor(
        IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX + "_a");
    try {
      admin.createTable(tableDesc);
      Assert.fail();
    } catch (IOException e) {
      // TODO: Fixme
      // Assert.assertTrue(e.getMessage().indexOf("is preserved") >= 0);
    }
    
    byte[] testMainTable = Bytes.toBytes("temp_test");
    byte[] testIndexTable = Bytes.toBytes("__themis_index_temp_test_ThemisCF_Qualifier_test_index");

    deleteTable(admin, testMainTable);
    // create themis table without index enable
    tableDesc = new HTableDescriptor(testMainTable);
    tableDesc.addFamily(IndexMasterObserver.getSecondaryIndexFamily());
    admin.createTable(tableDesc);
    HTableDescriptor[] tableDescs = admin.listTables();
    for (HTableDescriptor desc : tableDescs) {
      Assert.assertFalse(Bytes.equals(testIndexTable, desc.getName()));
    }
    admin.disableTable(testMainTable);
    admin.deleteTable(testMainTable);
    
    // create table with index attribute but without themis enable key
    tableDesc = new HTableDescriptor(testMainTable);
    HColumnDescriptor columnDesc = new HColumnDescriptor(INDEX_FAMILY);
    columnDesc.setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY,
      Boolean.TRUE.toString());
    tableDesc.addFamily(columnDesc);
    try {
      admin.createTable(tableDesc);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("must be set on themis-enabled family") >= 0);
    }
    
    // create themis table with secondary index attribute
    
    createTableForIndexTest(testMainTable);
    HTableDescriptor desc = admin.getTableDescriptor(testMainTable);
    Assert.assertNotNull(desc);
    Assert.assertNotNull(desc.getFamily(ColumnUtil.LOCK_FAMILY_NAME));
    desc = admin.getTableDescriptor(testIndexTable);
    Assert.assertNotNull(desc);
    Assert.assertNotNull(desc.getFamily(Bytes
        .toBytes(IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY)));
    Assert.assertNotNull(desc.getFamily(ColumnUtil.LOCK_FAMILY_NAME));
    
    deleteTableForIndexTest(testMainTable);
    tableDescs = admin.listTables();
    for (HTableDescriptor tempDesc : tableDescs) {
      if (Bytes.equals(tempDesc.getName(), testMainTable)) {
        Assert.fail("fail to delete table:" + Bytes.toString(testMainTable));
      } else if (Bytes.equals(tempDesc.getName(), testIndexTable)) {
        Assert.fail("fail to delete table:" + Bytes.toString(testIndexTable));
      }
    }
  }
}
