package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

public class TransactionTestBase extends TestBase {
  public static final int TEST_LOCK_CLEAN_RETRY_COUNT = 2;
  public static final int TEST_LOCK_CLEAN_PAUSE = 200;
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  
  protected HConnection connection;
  protected HTableInterface table;
  protected HTableInterface anotherTable;

  protected static Configuration conf;
  // the following ts has effect across uts
  protected static long timestampBase;
  protected static long prewriteTs;
  protected static long wallTime;
  protected static long commitTs;
  protected ThemisCoprocessorClient cpClient;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    /*
    conf = TEST_UTIL.getConfiguration();
    conf.setStrings("hbase.coprocessor.user.region.classes", ThemisProtocolImpl.class.getName());
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, ThemisScanObserver.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    // We need more than one region server in this test
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();
    TEST_UTIL.createTable(TABLENAME, new byte[][] { ColumnUtil.LOCK_FAMILY_NAME, FAMILY, ANOTHER_FAMILY });
    TEST_UTIL.createTable(ANOTHER_TABLENAME, new byte[][] { ColumnUtil.LOCK_FAMILY_NAME, FAMILY, ANOTHER_FAMILY });
    */
    conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
    conf.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.WritableRpcEngine");
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  @Before
  public void initEnv() throws IOException {
    connection = HConnectionManager.createConnection(conf);
    table = connection.getTable(TABLENAME);
    anotherTable = connection.getTable(ANOTHER_TABLENAME);
    deleteOldDataAndUpdateTs();
    cpClient = new ThemisCoprocessorClient(connection);
  }
  
  protected void deleteOldDataAndUpdateTs() throws IOException {
    nextTransactionTs();
    for (byte[] row : new byte[][] { ROW, ANOTHER_ROW, ZZ_ROW }) {
      for (HTableInterface hTable : new HTableInterface[] { table, anotherTable }) {
        hTable.delete(new Delete(row).deleteFamily(FAMILY, timestampBase)
            .deleteFamily(ANOTHER_FAMILY, timestampBase)
            .deleteFamily(ColumnUtil.LOCK_FAMILY_NAME, timestampBase));
      }
    }
  }
  
  protected HTableInterface getTable(byte[] tableName) throws IOException {
    if (Bytes.equals(TABLENAME, tableName)) {
      return table;
    } else if (Bytes.equals(ANOTHER_TABLENAME, tableName)) {
      return anotherTable;
    } else {
      throw new IOException("unknow table name, " + Bytes.toString(tableName));
    }
  }
  
  @After
  public void tearUp() throws IOException {
    table.close();
    anotherTable.close();
    connection.close();
  }
 
  protected void nextTransactionTs() {
    timestampBase = timestampBase == 0 ? System.currentTimeMillis() : timestampBase + 100;
    prewriteTs = timestampBase + 1;
    wallTime = prewriteTs;
    commitTs = prewriteTs + 1;
  }
  
  protected long lastTs(long ts) {
    return ts - 100;
  }
  
  // help methods for coprocessor-read test
  protected void commitOneColumn(ColumnCoordinate c, Type type) throws IOException {
    commitOneColumn(c, type, prewriteTs, commitTs);
  }
  
  protected void commitOneColumn(ColumnCoordinate c, Type type, long prewriteTs, long commitTs)
      throws IOException {
    if (type.equals(Type.Put)) {
      writeData(c, prewriteTs);
      writePutColumn(c, prewriteTs, commitTs);
    } else {
      writeDeleteAfterPut(c, prewriteTs, commitTs);
    }    
  }
  
  protected ThemisLock getLock(ColumnCoordinate c) throws IOException {
    return getLock(c, prewriteTs);
  }
  
  protected ThemisLock getLock(ColumnCoordinate c, long ts) throws IOException {
    return getLock(c, ts, false);
  }
  
  protected ThemisLock getLock(ColumnCoordinate c, long ts, boolean singleRow) throws IOException {
    ThemisLock lock =  COLUMN.equals(c) ? getPrimaryLock(ts, singleRow) : getSecondaryLock(c, ts);
    lock.setColumn(c);
    return lock;
  }
  
  protected void writeLockAndData(ColumnCoordinate c) throws IOException {
    writeLockAndData(c, prewriteTs);
  }
  
  protected void writeLockAndData(ColumnCoordinate c, long prewriteTs) throws IOException {
    Type type = getColumnType(c);
    if (type.equals(Type.Put)) {
      writeData(c, prewriteTs);
    }
    Column lc = ColumnUtil.getLockColumn(c);
    HTableInterface table = getTable(c.getTableName());
    byte[] lockBytes = ThemisLock.toByte(getLock(c, prewriteTs));
    table.put(new Put(c.getRow()).add(lc.getFamily(), lc.getQualifier(), prewriteTs, lockBytes));        
  }
  
  protected void writePutAndData(ColumnCoordinate c, long prewriteTs, long commitTs) throws IOException {
    writeData(c, prewriteTs);
    writePutColumn(c, prewriteTs, commitTs);
  }
  
  protected void writeData(ColumnCoordinate c, long prewriteTs) throws IOException {
    writeData(c, prewriteTs, VALUE);
  }
  
  public void writeData(ColumnCoordinate c, long prewriteTs, byte[] value) throws IOException {
    HTableInterface table = getTable(c.getTableName());
    table.put(new Put(c.getRow()).add(c.getFamily(), c.getQualifier(), prewriteTs, value));
  }
  
  protected void writePutColumn(ColumnCoordinate c, long prewriteTs, long commitTs) throws IOException {
    ColumnCoordinate putColumn = new ColumnCoordinate(c.getTableName(), c.getRow(), ColumnUtil.getPutColumn(c));
    writeWriteColumnInternal(putColumn, prewriteTs, commitTs);
  }
  
  protected void writeDeleteColumn(ColumnCoordinate c, long prewriteTs, long commitTs) throws IOException {
    ColumnCoordinate deleteColumn = new ColumnCoordinate(c.getTableName(), c.getRow(), ColumnUtil.getPutColumn(c));
    writeWriteColumnInternal(deleteColumn, prewriteTs, commitTs);
  }
  
  protected void writeWriteColumn(ColumnCoordinate c, long prewriteTs, long commitTs, boolean isPut) throws IOException {
    if (isPut) {
      writePutColumn(c, prewriteTs, commitTs);
    } else {
      writeDeleteColumn(c, prewriteTs, commitTs);
    }
  }
  
  protected void writePutAfterDelete(ColumnCoordinate c, long prewriteTs, long commitTs)
      throws IOException {
    writeDeleteColumn(c, prewriteTs - 3, commitTs - 3);
    writePutAndData(c, prewriteTs, commitTs);
  }
  
  protected void writeDeleteAfterPut(ColumnCoordinate c, long prewriteTs, long commitTs)
      throws IOException {
    writePutAndData(c, prewriteTs - 3, commitTs - 3);
    writeDeleteColumn(c, prewriteTs, commitTs);
  }
  
  private void writeWriteColumnInternal(ColumnCoordinate c, long prewriteTs, long commitTs) throws IOException {
    Put put = new Put(c.getRow()).add(c.getFamily(), c.getQualifier(), commitTs,
      Bytes.toBytes(prewriteTs));
    getTable(c.getTableName()).put(put);
  }
  
  protected byte[] readLockBytes(ColumnCoordinate c) throws IOException {
    return readLockBytes(c, prewriteTs);
  }
  
  protected byte[] readLockBytes(ColumnCoordinate c, long prewriteTs) throws IOException {
    ColumnCoordinate lc = new ColumnCoordinate(c.getTableName(), c.getRow(), ColumnUtil.getLockColumn(c));
    return readDataValue(lc, prewriteTs);
  }
  
  protected Long readPut(ColumnCoordinate c) throws IOException {
    return readCommitColumn(new ColumnCoordinate(c.getTableName(), c.getRow(), ColumnUtil.getPutColumn(c)));
  }
  
  protected Long readDelete(ColumnCoordinate c) throws IOException {
    return readCommitColumn(new ColumnCoordinate(c.getTableName(), c.getRow(), ColumnUtil.getDeleteColumn(c)));
  }
  
  protected Long readWrite(ColumnCoordinate c) throws IOException {
    if (getColumnType(c).equals(Type.Put)) {
      return readPut(c);
    } else {
      return readDelete(c);
    }
  }
  
  private Long readCommitColumn(ColumnCoordinate columnCoordinate) throws IOException {
    byte[] data = readDataValue(columnCoordinate, commitTs);
    return data == null ? null : Bytes.toLong(data);    
  }
  
  protected byte[] readDataValue(ColumnCoordinate c, long ts) throws IOException {
    Result result = readData(c, ts);
    if (result.list() == null || result.list().size() == 0) {
      return null;
    }
    return result.list().get(0).getValue();
  }
  
  protected Result readData(ColumnCoordinate c, long ts) throws IOException {
    Get get = new Get(c.getRow()).addColumn(c.getFamily(), c.getQualifier());
    get.setTimeStamp(ts);
    Result result = getTable(c.getTableName()).get(get);
    return result;
  }
  
  protected void eraseLock(ColumnCoordinate c, long ts) throws IOException {
    Column lc = ColumnUtil.getLockColumn(c);
    Delete delete = new Delete(c.getRow()).deleteColumn(lc.getFamily(), lc.getQualifier(), ts);
    getTable(c.getTableName()).delete(delete);
  }
  
  // help method for coprocessor write methods
  protected void checkCommitColumnSuccess(ColumnCoordinate c) throws IOException {
    Assert.assertNull(readLockBytes(c));
    if (getColumnType(c).equals(Type.Put)) {
      Assert.assertArrayEquals(VALUE, readDataValue(c, prewriteTs));
      Assert.assertEquals(prewriteTs, readPut(c).longValue());
      Assert.assertNull(readDelete(c));
    } else {
      Assert.assertNull(readLockBytes(c));
      Assert.assertNull(readPut(c));
      Assert.assertEquals(prewriteTs, readDelete(c).longValue());
    }
  }
  
  protected void checkPrewriteColumnSuccess(ColumnCoordinate c) throws IOException {
    checkPrewriteColumnSuccess(c, prewriteTs);
  }
  
  protected void checkPrewriteColumnSuccess(ColumnCoordinate c, boolean singleRow)
      throws IOException {
    checkPrewriteColumnSuccess(c, prewriteTs, singleRow);
  }
  
  protected void checkPrewriteColumnSuccess(ColumnCoordinate c, long prewriteTs) throws IOException {
    checkPrewriteColumnSuccess(c, prewriteTs, false);
  }
  
  protected void checkPrewriteColumnSuccess(ColumnCoordinate c, long prewriteTs, boolean singleRow)
      throws IOException {
    byte[] lockBytes = ThemisLock.toByte(getLock(c, prewriteTs, singleRow));
    Assert.assertArrayEquals(lockBytes, readLockBytes(c, prewriteTs));
    if (getColumnType(c).equals(Type.Put) && !singleRow) {
      Assert.assertArrayEquals(VALUE, readDataValue(c, prewriteTs));    
    } else {
      Assert.assertNull(readDataValue(c, prewriteTs));
    }        
  }
    
  protected void checkCommitRowSuccess(byte[] tableName, RowMutation rowMutation) throws IOException {
    for (ColumnMutation mutation : rowMutation.mutationList()) {
      ColumnCoordinate columnCoordinate = new ColumnCoordinate(tableName, rowMutation.getRow(), mutation);
      checkCommitColumnSuccess(columnCoordinate);
    }
  }
  
  protected void checkPrewriteRowSuccess(byte[] tableName, RowMutation rowMutation) throws IOException {
    checkPrewriteRowSuccess(tableName, rowMutation, false);
  }
  
  protected void checkPrewriteRowSuccess(byte[] tableName, RowMutation rowMutation,
      boolean singleRow) throws IOException {
    for (ColumnMutation mutation : rowMutation.mutationList()) {
      ColumnCoordinate columnCoordinate = new ColumnCoordinate(tableName, rowMutation.getRow(),
          mutation);
      checkPrewriteColumnSuccess(columnCoordinate, singleRow);
    }
  }
  
  protected ThemisLock prewritePrimaryRow() throws IOException {
    byte[] lockBytes = ThemisLock.toByte(getLock(COLUMN));
    ThemisLock lock = cpClient.prewriteRow(COLUMN.getTableName(), PRIMARY_ROW.getRow(),
      PRIMARY_ROW.mutationList(), prewriteTs, lockBytes, getSecondaryLockBytes(), 2);
    return lock;
  }
  
  protected ThemisLock prewriteSingleRow() throws IOException {
    byte[] lockBytes = ThemisLock.toByte(getLock(COLUMN, prewriteTs, true));
    ThemisLock lock = cpClient.prewriteSingleRow(COLUMN.getTableName(), PRIMARY_ROW.getRow(),
      PRIMARY_ROW.mutationListWithoutValue(), prewriteTs, lockBytes, getSecondaryLockBytes(), 2);
    return lock;
  }
  
  protected byte[] getSecondaryLockBytes() throws IOException {
    return ThemisLock.toByte(getLock(COLUMN_WITH_ANOTHER_TABLE));
  }
  
  protected void commitPrimaryRow() throws IOException {
    cpClient.commitRow(COLUMN.getTableName(), PRIMARY_ROW.getRow(),
      PRIMARY_ROW.mutationListWithoutValue(), prewriteTs, commitTs, 2);
  }
  
  protected void commitSingleRow() throws IOException {
    cpClient.commitSingleRow(COLUMN.getTableName(), PRIMARY_ROW.getRow(),
      PRIMARY_ROW.mutationList(), prewriteTs, commitTs, 2);
  }

  protected List<ThemisLock> prewriteSecondaryRows() throws IOException {
    List<ThemisLock> locks = new ArrayList<ThemisLock>();
    for (Pair<byte[], RowMutation> secondary : SECONDARY_ROWS) {
      RowMutation mutation = secondary.getSecond();
      ThemisLock lock = cpClient.prewriteSecondaryRow(secondary.getFirst(), mutation.getRow(),
        mutation.mutationList(), prewriteTs, getSecondaryLockBytes());
      locks.add(lock);
    }
    return locks;
  }
  
  protected void commitSecondaryRow() throws IOException {
    for (int i = 0; i < SECONDARY_ROWS.size(); ++i) {
      byte[] tableName = SECONDARY_ROWS.get(i).getFirst();
      RowMutation mutation = SECONDARY_ROWS.get(i).getSecond();
      cpClient.commitSecondaryRow(tableName, mutation.getRow(),
        mutation.mutationListWithoutValue(), prewriteTs, commitTs);
    }
  }
  
  protected void commitTestTransaction() throws IOException {
    prewritePrimaryRow();
    prewriteSecondaryRows();
    commitPrimaryRow();
    commitSecondaryRow();
  }
}
