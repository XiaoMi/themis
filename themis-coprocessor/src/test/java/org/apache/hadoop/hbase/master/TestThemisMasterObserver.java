package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.ServerLockCleaner;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpointClient;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL.TimestampType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestThemisMasterObserver extends TransactionTestBase {
  private HBaseAdmin admin = null;
  private byte[] testTable = Bytes.toBytes("test_table");
  private byte[] testFamily = Bytes.toBytes("test_family");

  @Before
  public void initEnv() throws IOException {
    super.initEnv();
    admin = new HBaseAdmin(conf);
  }
  
  @After
  public void tearUp() throws IOException {
    if (admin != null) {
      admin.close();
    }
    super.tearUp();
  }
  
  @Test
  public void testCreateThemisLockFamily() throws Exception {
    HColumnDescriptor columnDesc = ThemisMasterObserver
        .createLockFamily(HColumnDescriptor.DEFAULT_REPLICATION_SCOPE);
    checkLockFamilyDesc(columnDesc);
  }
  
  @Test
  public void testGetThemisCommitFamily() throws Exception {
    for (byte[] family : ColumnUtil.COMMIT_FAMILY_NAME_BYTES) {
      HColumnDescriptor columnDesc = ThemisMasterObserver.getCommitFamily(family,
        HColumnDescriptor.DEFAULT_REPLICATION_SCOPE);
      checkCommitFamilyDesc(columnDesc);
    }
  }

  @Test
  public void testAddLockFamilyForThemisTable() throws Exception {
    // create table without setting THEMIS_ENABLE
    deleteTable(admin, testTable);
    createTestTable(false);
    Assert
        .assertFalse(admin.getTableDescriptor(testTable).getFamiliesKeys().contains(ColumnUtil.LOCK_FAMILY_NAME));

    // create table with THEMIS_ENABLE and family 'L'
    deleteTable(admin, testTable);
    HTableDescriptor tableDesc = getTestTableDesc(true);
    HColumnDescriptor columnDesc = new HColumnDescriptor(ColumnUtil.LOCK_FAMILY_NAME);
    tableDesc.addFamily(columnDesc);
    try {
      admin.createTable(tableDesc);
      Assert.fail();
    } catch (DoNotRetryIOException e) {
      Assert.assertTrue(e.getMessage().indexOf("is preserved by themis when THEMIS_ENABLE is true") >= 0);
    }

    // create table with THEMIS_ENABLE
    deleteTable(admin, testTable);
    createTestTable(true);
    Assert.assertTrue(admin.getTableDescriptor(testTable).getFamiliesKeys()
      .contains(ColumnUtil.LOCK_FAMILY_NAME));
    tableDesc = admin.getTableDescriptor(testTable);
    checkLockFamilyDesc(tableDesc);
    if (ColumnUtil.isCommitToSameFamily()) {
      Assert.assertTrue(tableDesc.getFamily(ColumnUtil.PUT_FAMILY_NAME_BYTES) == null);
      Assert.assertTrue(tableDesc.getFamily(ColumnUtil.DELETE_FAMILY_NAME_BYTES) == null);
    }
    HColumnDescriptor dataColumnDesc = tableDesc.getFamily(testFamily);
    Assert.assertEquals(Integer.MAX_VALUE, dataColumnDesc.getMaxVersions());
    Assert.assertEquals(HConstants.FOREVER, dataColumnDesc.getTimeToLive());
    // exception when set MaxVersion
    deleteTable(admin, testTable);
    tableDesc = getTestTableDesc(true);
    tableDesc.getFamily(testFamily).setMaxVersions(1);
    try {
      admin.createTable(tableDesc);
    } catch (DoNotRetryIOException e) {
      Assert.assertTrue(e.getMessage().indexOf("can not set MaxVersion for family") >= 0);
    }
    deleteTable(admin, testTable);
    // exception when set TTL
    tableDesc = getTestTableDesc(true);
    tableDesc.getFamily(testFamily).setTimeToLive(60 * 1000);
    try {
      admin.createTable(tableDesc);
    } catch (DoNotRetryIOException e) {
      Assert.assertTrue(e.getMessage().indexOf("can not set TTL for family") >= 0);
    }
    deleteTable(admin, testTable);
  }

  @Test
  public void testAddCommitFamilyForThemisTable() throws Exception {
    if (ColumnUtil.isCommitToDifferentFamily()) {
      deleteTable(admin, testTable);
      createTestTable(true);
      HTableDescriptor tableDesc = admin.getTableDescriptor(testTable);
      checkLockFamilyDesc(tableDesc);
      checkCommitFamilyDesc(tableDesc);
      HColumnDescriptor dataColumnDesc = tableDesc.getFamily(testFamily);
      Assert.assertEquals(Integer.MAX_VALUE, dataColumnDesc.getMaxVersions());
      Assert.assertEquals(HConstants.FOREVER, dataColumnDesc.getTimeToLive());
    }
  }
  
  @Test
  public void testTruncateTable() throws IOException {
    deleteTable(admin, testTable);
    createTestTable(true);
    HTableInterface table = connection.getTable(testTable);
    HTableDescriptor desc = table.getTableDescriptor();
    admin.disableTable(testTable);
    admin.deleteTable(testTable);
    admin.createTable(desc);
    checkLockFamilyDesc(admin.getTableDescriptor(testTable));
    table.close();
    deleteTable(admin, testTable);
  }
  
  @Test
  public void testSetExpiredTsToZk() throws Exception {
    long ts = System.currentTimeMillis() - 10l * 86400 * 1000;
    ThemisMasterObserver masterObserver = new ThemisMasterObserver();
    masterObserver.zk = new ZooKeeperWatcher(conf, "test", null, true);
    masterObserver.themisExpiredTsZNodePath = ThemisMasterObserver.getThemisExpiredTsZNodePath(masterObserver.zk);
    masterObserver.setExpiredTsToZk(ts);
    Assert.assertEquals(ts, ThemisMasterObserver.getThemisExpiredTsFromZk(masterObserver.zk));
    
    // test get data from not-exist path
    Assert.assertEquals(Long.MIN_VALUE,
      ThemisMasterObserver.getThemisExpiredTsFromZk(masterObserver.zk,
        masterObserver.themisExpiredTsZNodePath + "/" + System.currentTimeMillis()));
    masterObserver.zk.close();
  }
  
  @Test
  public void testGetThemisTables() throws IOException {
    List<String> themisTableNames = ThemisMasterObserver.getThemisTables(connection);
    Assert.assertTrue(themisTableNames.contains(Bytes.toString(TABLENAME)));
    Assert.assertTrue(themisTableNames.contains(Bytes.toString(ANOTHER_TABLENAME)));
  }
  
  @Test
  public void testCleanTimeExpiredLock() throws IOException {
    ThemisMasterObserver masterObserver = new ThemisMasterObserver();
    masterObserver.connection = connection;
    masterObserver.lockCleaner = new ServerLockCleaner(masterObserver.connection,
      new ThemisEndpointClient(connection));
    writeLockAndData(COLUMN, prewriteTs);
    writeLockAndData(COLUMN_WITH_ANOTHER_TABLE, prewriteTs + 1);
    // won't clear lock
    masterObserver.cleanLockBeforeTimestamp(prewriteTs);
    checkPrewriteColumnSuccess(COLUMN, prewriteTs);
    checkPrewriteColumnSuccess(COLUMN_WITH_ANOTHER_TABLE, prewriteTs + 1);
    
    // will clear lock of one COLUMN
    masterObserver.cleanLockBeforeTimestamp(prewriteTs + 1);
    Assert.assertNull(readLockBytes(COLUMN, prewriteTs));
    checkPrewriteColumnSuccess(COLUMN_WITH_ANOTHER_TABLE, prewriteTs + 1);
    
    // will clear locks for both COLUMNs
    writeLockAndData(COLUMN, prewriteTs + 1);
    masterObserver.cleanLockBeforeTimestamp(prewriteTs + 2);
    Assert.assertNull(readLockBytes(COLUMN, prewriteTs + 1));
    Assert.assertNull(readLockBytes(COLUMN_WITH_ANOTHER_TABLE, prewriteTs + 1));
    
    writeLockAndData(COLUMN, prewriteTs + 100);
  }

  @Test
  public void testAlterThemisTable() throws IOException {
    // create table with THEMIS_ENABLE
    deleteTable(admin, testTable);
    createTestTable(true);
    HTableDescriptor tableDesc = admin.getTableDescriptor(testTable);
    checkLockFamilyDesc(tableDesc);
    if (ColumnUtil.isCommitToDifferentFamily()) {
      checkCommitFamilyDesc(tableDesc);
    }
    for (HColumnDescriptor columnDesc : tableDesc.getColumnFamilies()) {
      Assert.assertEquals(HConstants.REPLICATION_SCOPE_LOCAL, columnDesc.getScope());
    }

    // Alter table replication scope to global
    for (HColumnDescriptor columnDesc : tableDesc.getColumnFamilies()) {
      columnDesc.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    }
    admin.modifyTable(testTable, tableDesc);
    tableDesc = admin.getTableDescriptor(testTable);
    checkLockFamilyDesc(tableDesc);
    if (ColumnUtil.isCommitToDifferentFamily()) {
      checkCommitFamilyDesc(tableDesc);
    }
    for (HColumnDescriptor columnDesc : tableDesc.getColumnFamilies()) {
      Assert.assertEquals(HConstants.REPLICATION_SCOPE_GLOBAL, columnDesc.getScope());
    }

    // Remove RETURNED_THEMIS_TABLE_DESC and lock/commit family to test the table desc from libsds
    tableDesc.remove(ThemisMasterObserver.RETURNED_THEMIS_TABLE_DESC);
    tableDesc.removeFamily(ColumnUtil.LOCK_FAMILY_NAME);
    if (ColumnUtil.isCommitToDifferentFamily()) {
      for (byte[] family : ColumnUtil.COMMIT_FAMILY_NAME_BYTES) {
        tableDesc.removeFamily(family);
      }
    }
    // Alter table replication scope to local
    for (HColumnDescriptor columnDesc : tableDesc.getColumnFamilies()) {
      columnDesc.setScope(HConstants.REPLICATION_SCOPE_LOCAL);
    }
    admin.modifyTable(testTable, tableDesc);
    tableDesc = admin.getTableDescriptor(testTable);
    checkLockFamilyDesc(tableDesc);
    if (ColumnUtil.isCommitToDifferentFamily()) {
      checkCommitFamilyDesc(tableDesc);
    }
    for (HColumnDescriptor columnDesc : tableDesc.getColumnFamilies()) {
      Assert.assertEquals(HConstants.REPLICATION_SCOPE_LOCAL, columnDesc.getScope());
    }
  }

  private void createTestTable(boolean themisEnable) throws IOException {
    admin.createTable(getTestTableDesc(themisEnable));
  }

  private HTableDescriptor getTestTableDesc(boolean themisEnable) throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(testTable);
    HColumnDescriptor columnDesc = new HColumnDescriptor(testFamily);
    if (themisEnable) {
      columnDesc.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, "true");
    }
    tableDesc.addFamily(columnDesc);
    return tableDesc;
  }

  private void checkLockFamilyDesc(HTableDescriptor tableDescriptor) {
    Assert.assertNotNull(
        "Table should have lock family " + Bytes.toString(ColumnUtil.LOCK_FAMILY_NAME),
        tableDescriptor.getFamily(ColumnUtil.LOCK_FAMILY_NAME));
    checkLockFamilyDesc(tableDescriptor.getFamily(ColumnUtil.LOCK_FAMILY_NAME));
  }

  private void checkLockFamilyDesc(HColumnDescriptor columnDesc) {
    Assert.assertArrayEquals(ColumnUtil.LOCK_FAMILY_NAME, columnDesc.getName());
    Assert.assertEquals(1, columnDesc.getMaxVersions());
    Assert.assertTrue(columnDesc.isInMemory());
    Assert.assertEquals(HConstants.FOREVER, columnDesc.getTimeToLive());
  }

  private void checkCommitFamilyDesc(HTableDescriptor desc) {
    for (byte[] family : ColumnUtil.COMMIT_FAMILY_NAME_BYTES) {
      Assert.assertNotNull("Table should have commit family " + Bytes.toString(family),
          desc.getFamily(family));
      checkCommitFamilyDesc(desc.getFamily(family));
    }
  }

  private void checkCommitFamilyDesc(HColumnDescriptor columnDesc) {
    Assert.assertEquals(Integer.MAX_VALUE, columnDesc.getMaxVersions());
    Assert.assertEquals(HConstants.FOREVER, columnDesc.getTimeToLive());
  }
}
