package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.apache.hadoop.hbase.themis.exception.WriteConflictException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestThemisCoprocessorWrite extends TransactionTestBase {
  protected static byte[][] primaryFamilies;
  protected static byte[][] primaryQualifiers;
  protected static byte[] primaryTypes;
  protected static byte[][][] secondaryFamilies;
  protected static byte[][][] secondaryQualifiers;
  protected static byte[][] secondaryTypes;
  
  @Test
  public void testCheckPrewritePrimaryRowSuccess() throws Exception {
    Assert.assertNull(prewritePrimaryRow());
    checkPrewriteRowSuccess(TABLENAME, PRIMARY_ROW);
  }
  
  @Test
  public void testCheckPrewriteSingleRowSuccess() throws Exception {
    Assert.assertNull(prewriteSingleRow());
    checkPrewriteRowSuccess(TABLENAME, PRIMARY_ROW, true);
  }
  
  @Test
  public void testCheckPrewriteSecondaryRowSuccess() throws Exception {
    List<ThemisLock> prewriteLocks = prewriteSecondaryRows();
    for (int i = 0; i < prewriteLocks.size(); ++i) {
      Assert.assertNull(prewriteLocks.get(i));
      checkPrewriteRowSuccess(SECONDARY_ROWS.get(i).getFirst(), SECONDARY_ROWS.get(i).getSecond());
    }
  }
    
  @Test
  public void testCommitPrimaryRowSuccess() throws Exception {
    Assert.assertNull(prewritePrimaryRow());
    commitPrimaryRow();
    checkCommitRowSuccess(COLUMN.getTableName(), PRIMARY_ROW);
  }
  
  @Test
  public void testCommitSingleRowSuccess() throws Exception {
    Assert.assertNull(prewriteSingleRow());
    commitSingleRow();
    checkCommitRowSuccess(COLUMN.getTableName(), PRIMARY_ROW);
  }
  
  @Test
  public void testCommitSingleRowFail() throws Exception {
    prewriteSingleRow();
    eraseLock(COLUMN, prewriteTs);
    try {
      commitSingleRow();
      Assert.fail();
    } catch (LockCleanedException e) {}
    for (ColumnCoordinate columnCoordinate : new ColumnCoordinate[] { COLUMN,
        COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER }) {
      Assert.assertNull(readWrite(columnCoordinate));
      Assert.assertNull(readDataValue(columnCoordinate, prewriteTs));
    }
  }
  
  @Test
  public void testCommitPrimaryRowFail() throws Exception {
    prewritePrimaryRow();
    // will commit if lock of non-primary column has been erased
    eraseLock(COLUMN_WITH_ANOTHER_FAMILY, prewriteTs);
    commitPrimaryRow();
    checkCommitRowSuccess(COLUMN.getTableName(), PRIMARY_ROW);
    
    nextTransactionTs();
    deleteOldDataAndUpdateTs();
    prewritePrimaryRow();
    eraseLock(COLUMN, prewriteTs);
    try {
      commitPrimaryRow();
      Assert.fail();
    } catch (LockCleanedException e) {}
    for (ColumnCoordinate columnCoordinate : new ColumnCoordinate[] { COLUMN,
        COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER }) {
      Assert.assertNull(readWrite(columnCoordinate));
      if (getColumnType(columnCoordinate) == Type.Put) {
        Assert.assertNotNull(readDataValue(columnCoordinate, prewriteTs));
      }
    }
  }
  
  @Test
  public void testCommitSecondaryRowSuccess() throws Exception {
    List<ThemisLock> prewriteLocks = prewriteSecondaryRows();
    commitSecondaryRow();
    for (int i = 0; i < prewriteLocks.size(); ++i) {
      Assert.assertNull(prewriteLocks.get(i));
      checkCommitRowSuccess(SECONDARY_ROWS.get(i).getFirst(), SECONDARY_ROWS.get(i).getSecond());
    }
    
    // commit secondary success without lock
    nextTransactionTs();
    prewriteLocks = prewriteSecondaryRows();
    for (int i = 0; i < prewriteLocks.size(); ++i) {
      Assert.assertNull(prewriteLocks.get(i));
      for (Pair<byte[], RowMutation> tableEntry : SECONDARY_ROWS) {
        byte[] tableName = tableEntry.getFirst();
        byte[] row = tableEntry.getSecond().getRow();
        for (ColumnMutation columnMutation : tableEntry.getSecond().mutationList()) {
          ColumnCoordinate columnCoordinate = new ColumnCoordinate(tableName, row, columnMutation);
          eraseLock(columnCoordinate, prewriteTs);
        }
      }
    }
    commitSecondaryRow();
    for (int i = 0; i < prewriteLocks.size(); ++i) {
      checkCommitRowSuccess(SECONDARY_ROWS.get(i).getFirst(), SECONDARY_ROWS.get(i).getSecond());
    }
    
    // won't ttl for commitSecondary
    long expiredPrewriteTs = TransactionTTL.getExpiredTimestampForWrite(System.currentTimeMillis())
        - TransactionTTL.transactionTTLTimeError;
    Pair<byte[], RowMutation> secondary = SECONDARY_ROWS.get(0);
    RowMutation mutation = secondary.getSecond();
    cpClient.commitSecondaryRow(secondary.getFirst(), mutation.getRow(),
      mutation.mutationListWithoutValue(), expiredPrewriteTs, commitTs);
  }
  
  // the following unit tests are for prewrite/commit by columns, should be deprecated
  protected ThemisLock invokePrewriteRow(RowMutation rowMutation, long prewriteTs, int primaryIndex)
      throws IOException {
    byte[] lockBytes = ThemisLock.toByte(getLock(COLUMN));
    ThemisLock lock = cpClient.prewriteRow(COLUMN.getTableName(), PRIMARY_ROW.getRow(),
      PRIMARY_ROW.mutationList(), prewriteTs, lockBytes, getSecondaryLockBytes(), 2);
    return lock;
  }
  
  @Test
  public void testPrewriteRowWithLockConflict() throws IOException {
    // older lock
    writeLockAndData(COLUMN);
    nextTransactionTs();
    ThemisLock conflict = invokePrewriteRow(PRIMARY_ROW, commitTs, 2);
    Assert.assertNotNull(conflict);
    Assert.assertTrue(getLock(COLUMN, lastTs(prewriteTs)).equals(conflict));
    // newer lock
    deleteOldDataAndUpdateTs();
    writeLockAndData(COLUMN);
    conflict = invokePrewriteRow(PRIMARY_ROW, prewriteTs - 1, 2);
    Assert.assertNotNull(conflict);
    Assert.assertTrue(getLock(COLUMN).equals(conflict));
    
    // test expired lock
    if (TEST_UTIL != null) {
      TransactionTTL.init(conf);
      truncateTable(TABLENAME);
      long writeTs = TransactionTTL.getExpiredTimestampForWrite(System.currentTimeMillis()
          - TransactionTTL.transactionTTLTimeError);
      writeLockAndData(COLUMN, writeTs);
      conflict = invokePrewriteRow(PRIMARY_ROW, commitTs, 2);
      Assert.assertNotNull(conflict);
      Assert.assertTrue(getLock(COLUMN, writeTs).equals(conflict));
      Assert.assertTrue(conflict.isLockExpired());
    }
  }
  
  @Test
  public void testPrewriteColumnWithNewWriteConflict() throws IOException {
    // older write, won't cause write conflict
    commitTestTransaction();
    nextTransactionTs();

    ThemisLock conflict = invokePrewriteRow(PRIMARY_ROW, prewriteTs, 2);
    Assert.assertNull(conflict);
    checkPrewriteRowSuccess(TABLENAME, PRIMARY_ROW);
    
    // newer write, will throw exception
    deleteOldDataAndUpdateTs();
    commitTestTransaction();
    try {
      invokePrewriteRow(PRIMARY_ROW, commitTs - 1, 2);
      Assert.fail();
    } catch (WriteConflictException e) {
    }
  }
  
  @Test
  public void testPrewriteColumnWithConflict() throws IOException {
    // lock conflict and new write conflict
    commitTestTransaction();
    nextTransactionTs();
    writeLockAndData(COLUMN);
    try {
      invokePrewriteRow(PRIMARY_ROW, lastTs(commitTs) - 1, 2);
      Assert.fail();
    } catch (WriteConflictException e) {}
  }
  
  @Test
  public void testWriteNonThemisFamily() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    byte[] testTable = Bytes.toBytes("test_table");
    byte[] testFamily = Bytes.toBytes("test_family");

    // create table without setting THEMIS_ENABLE
    deleteTable(admin, testTable);
    HTableDescriptor tableDesc = new HTableDescriptor(testTable);
    HColumnDescriptor columnDesc = new HColumnDescriptor(testFamily);
    tableDesc.addFamily(columnDesc);
    admin.createTable(tableDesc);
    try {
      ColumnMutation mutation = new ColumnMutation(new Column(testFamily, COLUMN.getQualifier()),
          Type.Put, VALUE);
      cpClient.prewriteRow(testTable, PRIMARY_ROW.getRow(), Lists.newArrayList(mutation), prewriteTs,
        ThemisLock.toByte(getLock(COLUMN)), getSecondaryLockBytes(), 2);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(e.getMessage().indexOf("can not access family") >= 0);
    }
    admin.close();
  }
  
  @Test
  public void testTransactionExpiredWhenPrewrite() throws IOException {
    // only test in MiniCluster
    if (TEST_UTIL != null) {
      // remove old family delete
      truncateTable(TABLENAME);
      // won't expired
      long currentMs = System.currentTimeMillis() + TransactionTTL.writeTransactionTTL;
      prewriteTs = TransactionTTL.getExpiredTimestampForWrite(currentMs);
      prewritePrimaryRow();
      checkPrewriteRowSuccess(TABLENAME, PRIMARY_ROW);
      
      // make sure this transaction will be expired
      currentMs = System.currentTimeMillis() - TransactionTTL.transactionTTLTimeError;
      prewriteTs = TransactionTTL.getExpiredTimestampForWrite(currentMs);
      try {
        prewritePrimaryRow();
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().indexOf("Expired Write Transaction") >= 0);
      }
      // this ut will write chronos timestamp, need delete data by truncating table
      truncateTable(TABLENAME);
    }
  }
  
  @Test
  public void testTransactionExpiredWhenCommit() throws IOException {
    // only test in MiniCluster
    if (TEST_UTIL != null) {
      // remove old family delete
      truncateTable(TABLENAME);
      // won't expired
      long currentMs = System.currentTimeMillis() + TransactionTTL.writeTransactionTTL;
      prewriteTs = TransactionTTL.getExpiredTimestampForWrite(currentMs);
      Assert.assertNull(prewritePrimaryRow());
      checkPrewriteRowSuccess(TABLENAME, PRIMARY_ROW);
      commitPrimaryRow();

      // make sure this transaction will be expired
      currentMs = System.currentTimeMillis() - TransactionTTL.transactionTTLTimeError;
      prewriteTs = TransactionTTL.getExpiredTimestampForWrite(currentMs);
      try {
        commitPrimaryRow();
        Assert.fail();
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().indexOf("Expired Write Transaction") >= 0);
      }
      truncateTable(TABLENAME);
    }
  }
}