package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class TestThemisMasterObserver extends TransactionTestBase {
  @Test
  public void testCreateThemisLockFamily() throws Exception {
    HColumnDescriptor columnDesc = ThemisMasterObserver.createLockFamily();
    Assert.assertArrayEquals(ColumnUtil.LOCK_FAMILY_NAME, columnDesc.getName());
    Assert.assertEquals(1, columnDesc.getMaxVersions());
    Assert.assertTrue(columnDesc.isInMemory());
  }
  
  @Test
  public void testAddLockFamilyForThemisTable() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(conf);
    byte[] testTable = Bytes.toBytes("test_table");
    byte[] testFamily = Bytes.toBytes("test_family");

    // create table without setting THEMIS_ENABLE
    deleteTable(admin, testTable);
    HTableDescriptor tableDesc = new HTableDescriptor(testTable);
    HColumnDescriptor columnDesc = new HColumnDescriptor(testFamily);
    tableDesc.addFamily(columnDesc);
    admin.createTable(tableDesc);
    Assert
        .assertFalse(admin.getTableDescriptor(testTable).getFamiliesKeys().contains(ColumnUtil.LOCK_FAMILY_NAME));
    
    // create table with THEMIS_ENABLE and family 'L'
    deleteTable(admin, testTable);
    tableDesc = new HTableDescriptor(testTable);
    columnDesc = new HColumnDescriptor(testFamily);
    columnDesc.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, "true");
    tableDesc.addFamily(columnDesc);
    columnDesc = new HColumnDescriptor(ColumnUtil.LOCK_FAMILY_NAME);
    tableDesc.addFamily(columnDesc);
    try {
      admin.createTable(tableDesc);
      Assert.fail();
    } catch (DoNotRetryIOException e) {
      Assert.assertTrue(e.getMessage().indexOf("is preserved by themis when THEMIS_ENABLE is true") >= 0);
    }
    
    // create table with THEMIS_ENABLE
    deleteTable(admin, testTable);
    tableDesc = new HTableDescriptor(testTable);
    columnDesc = new HColumnDescriptor(testFamily);
    columnDesc.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, "true");
    tableDesc.addFamily(columnDesc);
    admin.createTable(tableDesc);
    Assert.assertTrue(admin.getTableDescriptor(testTable).getFamiliesKeys()
        .contains(ColumnUtil.LOCK_FAMILY_NAME));
    deleteTable(admin, testTable);
    admin.close();
  } 
}