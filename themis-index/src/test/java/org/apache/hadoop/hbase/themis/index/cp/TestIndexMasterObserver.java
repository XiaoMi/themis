package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
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
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(
          TableName.valueOf(IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX + "_a"))
          .build();
    try {
      admin.createTable(tableDesc);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("is preserved") >= 0);
    }
    
    byte[] testMainTable = Bytes.toBytes("temp_test");
    byte[] testIndexTable = Bytes.toBytes("__themis_index_temp_test_ThemisCF_Qualifier_test_index");
    final TableName testMainTableName = TableName.valueOf(testMainTable);
    final TableName testIndexTableName = TableName.valueOf(testIndexTable);

    deleteTable(admin, testMainTable);
    // create themis table without index enable
    tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(testMainTable))
            .setColumnFamily(IndexMasterObserver.getSecondaryIndexFamily())
            .build();
    admin.createTable(tableDesc);

    List<TableDescriptor> tableDescs = admin.listTableDescriptors();
    for (TableDescriptor desc : tableDescs) {
      Assert.assertFalse(Bytes.equals(testIndexTable, desc.getTableName().getName()));
    }
    admin.disableTable(testMainTableName);
    admin.deleteTable(testMainTableName);
    
    ColumnFamilyDescriptor columnDesc = ColumnFamilyDescriptorBuilder.of(INDEX_FAMILY);
    tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(testMainTable))
              .setColumnFamily(columnDesc)
              .setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY, Boolean.TRUE.toString())
              .build();
    try {
      admin.createTable(tableDesc);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("must be set on themis-enabled family") >= 0);
    }
    
    // create themis table with secondary index attribute

    createTableForIndexTest(testMainTable);
    TableDescriptor desc = admin.getDescriptor(testMainTableName);
    Assert.assertNotNull(desc);
    Assert.assertNotNull(desc.getColumnFamily(ColumnUtil.LOCK_FAMILY_NAME));

    desc = admin.getDescriptor(testIndexTableName);
    Assert.assertNotNull(desc);
    Assert.assertNotNull(desc.getColumnFamily(Bytes
        .toBytes(IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY)));
    Assert.assertNotNull(desc.getColumnFamily(ColumnUtil.LOCK_FAMILY_NAME));
    
    deleteTableForIndexTest(testMainTable);
    tableDescs = admin.listTableDescriptors();
    for (TableDescriptor tempDesc : tableDescs) {
      if (Bytes.equals(tempDesc.getTableName().getName(), testMainTable)) {
        Assert.fail("fail to delete table:" + Bytes.toString(testMainTable));
      } else if (Bytes.equals(tempDesc.getTableName().getName(), testIndexTable)) {
        Assert.fail("fail to delete table:" + Bytes.toString(testIndexTable));
      }
    }
  }
}