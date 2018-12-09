package org.apache.hadoop.hbase.themis.index.cp;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestIndexMasterObserver extends IndexTestBase {

  @Test
  public void testCheckIndexNames() throws IOException {
    ColumnFamilyDescriptorBuilder builder =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C"));
    String familyIndexAttribute = "index_a:a;index_a:a";

    builder.setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY,
      familyIndexAttribute);
    try {
      IndexMasterObserver.checkIndexNames(builder.build());
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("duplicate secondary index definition"));
    }

    String[] familyIndexAttributes = new String[] { "index_a:", ":index_a", ":" };
    for (String indexAttribute : familyIndexAttributes) {
      builder.setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY,
        indexAttribute);
      try {
        IndexMasterObserver.checkIndexNames(builder.build());
      } catch (IOException e) {
        assertThat(e.getMessage(), containsString("illegal secondary index definition"));
      }
    }

    familyIndexAttributes = new String[] { "index_a:a", "index_a:a;index_a:b;index_b:a" };
    for (String indexAttribute : familyIndexAttributes) {
      builder.setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY,
        indexAttribute);
      IndexMasterObserver.checkIndexNames(builder.build());
    }
  }

  @Test
  public void testConstructSecondaryIndexTableName() {
    assertEquals("__themis_index_a_b_c_d", IndexMasterObserver
      .constructSecondaryIndexTableName(TableName.valueOf("a"), "b", "c", "d").getNameAsString());
  }

  @Test
  public void testGetSecondaryIndexTableDesc() throws IOException {
    TableName indexTableName = TableName.valueOf("__themis_index_a_b_c_d");
    TableDescriptor desc = IndexMasterObserver.getSecondaryIndexTableDesc(indexTableName);
    assertNotNull(desc.getValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_ATTRIBUTE_KEY));
    ColumnFamilyDescriptor family =
      desc.getColumnFamily(Bytes.toBytes(IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY));
    assertNotNull(family);
    assertNotNull(family.getValue(ThemisMasterObserver.THEMIS_ENABLE_KEY_BYTES));
  }

  @Test
  public void testCreateDeleteTableWithSecondaryIndex() throws IOException {
    // create normal table with secondary index table prefix
    try {
      admin.createTable(TableDescriptorBuilder
        .newBuilder(
          TableName.valueOf(IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX + "_a"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("whatever")).build());
      fail();
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("is preserved"));
    }

    TableName testMainTable = TableName.valueOf("temp_test");
    TableName testIndexTable =
      TableName.valueOf("__themis_index_temp_test_ThemisCF_Qualifier_test_index");

    deleteTable(testMainTable);
    // create themis table without index enable
    admin.createTable(TableDescriptorBuilder.newBuilder(testMainTable)
      .setColumnFamily(IndexMasterObserver.getSecondaryIndexFamily()).build());
    for (TableDescriptor desc : admin.listTableDescriptors()) {
      assertNotEquals(testIndexTable, desc.getTableName());
    }
    admin.disableTable(testMainTable);
    admin.deleteTable(testMainTable);

    // create table with index attribute but without themis enable key
    try {
      admin.createTable(TableDescriptorBuilder.newBuilder(testMainTable)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(INDEX_FAMILY)
          .setValue(IndexMasterObserver.THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY,
            Boolean.TRUE.toString())
          .build())
        .build());
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("must be set on themis-enabled family"));
    }

    // create themis table with secondary index attribute
    createTableForIndexTest(testMainTable);
    TableDescriptor desc = admin.getDescriptor(testMainTable);
    assertNotNull(desc);
    assertNotNull(desc.getColumnFamily(ColumnUtil.LOCK_FAMILY_NAME));
    for (TableDescriptor t : admin.listTableDescriptors()) {
      System.err.println(t);
    }
    desc = admin.getDescriptor(testIndexTable);
    assertNotNull(desc);
    assertNotNull(
      desc.getColumnFamily(Bytes.toBytes(IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY)));
    assertNotNull(desc.getColumnFamily(ColumnUtil.LOCK_FAMILY_NAME));

    deleteTableForIndexTest(testMainTable);
    for (TableDescriptor tempDesc : admin.listTableDescriptors()) {
      assertNotEquals("fail to delete table:" + testMainTable, testMainTable,
        tempDesc.getTableName());
      assertNotEquals("fail to delete table:" + testIndexTable, testIndexTable,
        tempDesc.getTableName());
    }
  }
}
