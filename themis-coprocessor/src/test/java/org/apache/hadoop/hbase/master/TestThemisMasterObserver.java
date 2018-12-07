package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.ServerLockCleaner;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpointClient;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.xiaomi.infra.thirdparty.com.google.common.io.Closeables;

public class TestThemisMasterObserver extends TransactionTestBase {
  private Admin admin = null;
  private TableName testTable = TableName.valueOf("test_table");
  private byte[] testFamily = Bytes.toBytes("test_family");

  @Before
  public void initEnv() throws IOException {
    super.initEnv();
    admin = connection.getAdmin();
  }

  @After
  public void tearUp() throws IOException {
    Closeables.close(admin, true);
    super.tearUp();
  }

  @Test
  public void testCreateThemisLockFamily() throws Exception {
    ColumnFamilyDescriptor columnDesc = ThemisMasterObserver
      .createLockFamily(ColumnFamilyDescriptorBuilder.DEFAULT_REPLICATION_SCOPE);
    checkLockFamilyDesc(columnDesc);
  }

  @Test
  public void testGetThemisCommitFamily() throws Exception {
    for (byte[] family : ColumnUtil.COMMIT_FAMILY_NAME_BYTES) {
      ColumnFamilyDescriptor columnDesc = ThemisMasterObserver.getCommitFamily(family,
        ColumnFamilyDescriptorBuilder.DEFAULT_REPLICATION_SCOPE);
      checkCommitFamilyDesc(columnDesc);
    }
  }

  @Test
  public void testAddLockFamilyForThemisTable() throws Exception {
    // create table without setting THEMIS_ENABLE
    deleteTable(testTable);
    createTestTable(false);

    assertFalse(
      admin.getDescriptor(testTable).getColumnFamilyNames().contains(ColumnUtil.LOCK_FAMILY_NAME));

    // create table with THEMIS_ENABLE and family 'L'
    deleteTable(testTable);
    HTableDescriptor tableDesc = getTestTableDesc(true);
    HColumnDescriptor columnDesc = new HColumnDescriptor(ColumnUtil.LOCK_FAMILY_NAME);
    tableDesc.addFamily(columnDesc);
    try {
      admin.createTable(tableDesc);
      fail();
    } catch (DoNotRetryIOException e) {
      assertTrue(e.getMessage().indexOf("is preserved by themis when THEMIS_ENABLE is true") >= 0);
    }

    // create table with THEMIS_ENABLE
    deleteTable(testTable);
    createTestTable(true);
    assertTrue(
      admin.getDescriptor(testTable).getColumnFamilyNames().contains(ColumnUtil.LOCK_FAMILY_NAME));
    tableDesc = admin.getTableDescriptor(testTable);
    checkLockFamilyDesc(tableDesc);
    if (ColumnUtil.isCommitToSameFamily()) {
      assertTrue(tableDesc.getFamily(ColumnUtil.PUT_FAMILY_NAME_BYTES) == null);
      assertTrue(tableDesc.getFamily(ColumnUtil.DELETE_FAMILY_NAME_BYTES) == null);
    }
    HColumnDescriptor dataColumnDesc = tableDesc.getFamily(testFamily);
    assertEquals(Integer.MAX_VALUE, dataColumnDesc.getMaxVersions());
    assertEquals(HConstants.FOREVER, dataColumnDesc.getTimeToLive());
    // exception when set MaxVersion
    deleteTable(testTable);
    tableDesc = getTestTableDesc(true);
    tableDesc.getFamily(testFamily).setMaxVersions(1);
    try {
      admin.createTable(tableDesc);
    } catch (DoNotRetryIOException e) {
      assertTrue(e.getMessage().indexOf("can not set MaxVersion for family") >= 0);
    }
    deleteTable(testTable);
    // exception when set TTL
    tableDesc = getTestTableDesc(true);
    tableDesc.getFamily(testFamily).setTimeToLive(60 * 1000);
    try {
      admin.createTable(tableDesc);
    } catch (DoNotRetryIOException e) {
      assertTrue(e.getMessage().indexOf("can not set TTL for family") >= 0);
    }
    deleteTable(testTable);
  }

  @Test
  public void testAddCommitFamilyForThemisTable() throws Exception {
    if (ColumnUtil.isCommitToDifferentFamily()) {
      deleteTable(testTable);
      createTestTable(true);
      HTableDescriptor tableDesc = admin.getTableDescriptor(testTable);
      checkLockFamilyDesc(tableDesc);
      checkCommitFamilyDesc(tableDesc);
      HColumnDescriptor dataColumnDesc = tableDesc.getFamily(testFamily);
      assertEquals(Integer.MAX_VALUE, dataColumnDesc.getMaxVersions());
      assertEquals(HConstants.FOREVER, dataColumnDesc.getTimeToLive());
    }
  }

  @Test
  public void testTruncateTable() throws IOException {
    deleteTable(testTable);
    createTestTable(true);
    admin.disableTable(testTable);
    admin.truncateTable(testTable, false);
    checkLockFamilyDesc(admin.getDescriptor(testTable));
    deleteTable(testTable);
  }

  @Test
  public void testSetExpiredTsToZk() throws Exception {
    long ts = System.currentTimeMillis() - 10l * 86400 * 1000;
    try (ZKWatcher zk = new ZKWatcher(conf, "test", null)) {
      ThemisMasterObserver masterObserver = new ThemisMasterObserver();
      masterObserver.services = new MockMasterServices(conf, connection, zk);
      masterObserver.themisExpiredTsZNodePath =
        ThemisMasterObserver.getThemisExpiredTsZNodePath(zk);
      masterObserver.setExpiredTsToZk(ts);
      assertEquals(ts, ThemisMasterObserver.getThemisExpiredTsFromZk(zk));

      // test get data from not-exist path
      assertEquals(Long.MIN_VALUE, ThemisMasterObserver.getThemisExpiredTsFromZk(zk,
        masterObserver.themisExpiredTsZNodePath + "/" + System.currentTimeMillis()));
    }
  }

  @Test
  public void testGetThemisTables() throws IOException {
    List<TableName> themisTableNames = ThemisMasterObserver.getThemisTables(connection);
    assertTrue(themisTableNames.contains(TABLENAME));
    assertTrue(themisTableNames.contains(ANOTHER_TABLENAME));
  }

  @Test
  public void testCleanTimeExpiredLock() throws IOException {
    ThemisMasterObserver masterObserver = new ThemisMasterObserver();
    masterObserver.services = new MockMasterServices(conf, connection, null);
    masterObserver.lockCleaner =
      new ServerLockCleaner(connection, new ThemisEndpointClient(connection));
    writeLockAndData(COLUMN, prewriteTs);
    writeLockAndData(COLUMN_WITH_ANOTHER_TABLE, prewriteTs + 1);
    // won't clear lock
    masterObserver.cleanLockBeforeTimestamp(prewriteTs);
    checkPrewriteColumnSuccess(COLUMN, prewriteTs);
    checkPrewriteColumnSuccess(COLUMN_WITH_ANOTHER_TABLE, prewriteTs + 1);

    // will clear lock of one COLUMN
    masterObserver.cleanLockBeforeTimestamp(prewriteTs + 1);
    assertNull(readLockBytes(COLUMN, prewriteTs));
    checkPrewriteColumnSuccess(COLUMN_WITH_ANOTHER_TABLE, prewriteTs + 1);

    // will clear locks for both COLUMNs
    writeLockAndData(COLUMN, prewriteTs + 1);
    masterObserver.cleanLockBeforeTimestamp(prewriteTs + 2);
    assertNull(readLockBytes(COLUMN, prewriteTs + 1));
    assertNull(readLockBytes(COLUMN_WITH_ANOTHER_TABLE, prewriteTs + 1));

    writeLockAndData(COLUMN, prewriteTs + 100);
  }

  // Re-enable after we find out how to deal with preModifyTable
  @Ignore
  @Test
  public void testAlterThemisTable() throws IOException {
    // create table with THEMIS_ENABLE
    deleteTable(testTable);
    createTestTable(true);
    HTableDescriptor tableDesc = admin.getTableDescriptor(testTable);
    checkLockFamilyDesc(tableDesc);
    if (ColumnUtil.isCommitToDifferentFamily()) {
      checkCommitFamilyDesc(tableDesc);
    }
    for (HColumnDescriptor columnDesc : tableDesc.getColumnFamilies()) {
      assertEquals(HConstants.REPLICATION_SCOPE_LOCAL, columnDesc.getScope());
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
      assertEquals(HConstants.REPLICATION_SCOPE_GLOBAL, columnDesc.getScope());
    }

    // Remove RETURNED_THEMIS_TABLE_DESC and lock/commit family to test the table desc from libsds
    // tableDesc.remove(ThemisMasterObserver.RETURNED_THEMIS_TABLE_DESC);
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
      assertEquals(HConstants.REPLICATION_SCOPE_LOCAL, columnDesc.getScope());
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

  private void checkLockFamilyDesc(TableDescriptor tableDescriptor) {
    assertNotNull("Table should have lock family " + Bytes.toString(ColumnUtil.LOCK_FAMILY_NAME),
      tableDescriptor.getColumnFamily(ColumnUtil.LOCK_FAMILY_NAME));
    checkLockFamilyDesc(tableDescriptor.getColumnFamily(ColumnUtil.LOCK_FAMILY_NAME));
  }

  private void checkLockFamilyDesc(ColumnFamilyDescriptor columnDesc) {
    assertArrayEquals(ColumnUtil.LOCK_FAMILY_NAME, columnDesc.getName());
    assertEquals(1, columnDesc.getMaxVersions());
    assertTrue(columnDesc.isInMemory());
    assertEquals(HConstants.FOREVER, columnDesc.getTimeToLive());
  }

  private void checkCommitFamilyDesc(TableDescriptor desc) {
    for (byte[] family : ColumnUtil.COMMIT_FAMILY_NAME_BYTES) {
      assertNotNull("Table should have commit family " + Bytes.toString(family),
        desc.getColumnFamily(family));
      checkCommitFamilyDesc(desc.getColumnFamily(family));
    }
  }

  private void checkCommitFamilyDesc(ColumnFamilyDescriptor columnDesc) {
    assertEquals(Integer.MAX_VALUE, columnDesc.getMaxVersions());
    assertEquals(HConstants.FOREVER, columnDesc.getTimeToLive());
  }
}
