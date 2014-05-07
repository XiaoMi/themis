package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.cache.ColumnMutationCache;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.exception.WriteConflictException;
import org.apache.hadoop.hbase.themis.lock.PrimaryLock;
import org.apache.hadoop.hbase.themis.lock.SecondaryLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

public class TestTransactionWrite extends ClientTestBase {
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    createTransactionWithMock();
  }
  
  @Test
  public void testLocalMutation() throws IOException {
    // add a put mutation
    transaction.put(TABLENAME, getThemisPut(COLUMN));
    ColumnMutationCache mutationCache = transaction.mutationCache;
    Assert.assertEquals(1, mutationCache.size());
    Assert.assertTrue(mutationCache.hasMutation(COLUMN));
    // add another put mutation into the same column
    transaction.put(TABLENAME, getThemisPut(COLUMN, ANOTHER_VALUE));
    mutationCache = transaction.mutationCache;
    Assert.assertEquals(1, mutationCache.size());
    Assert.assertTrue(mutationCache.hasMutation(COLUMN));
    Assert.assertTrue(Bytes.equals(ANOTHER_VALUE, mutationCache.getMutation(COLUMN).getSecond()));
    // put contains different columns
    transaction.put(TABLENAME, getThemisPut(COLUMN_WITH_ANOTHER_FAMILY));
    mutationCache = transaction.mutationCache;
    Assert.assertEquals(2, mutationCache.size());
    Assert.assertTrue(mutationCache.hasMutation(COLUMN));
    Assert.assertTrue(mutationCache.hasMutation(COLUMN_WITH_ANOTHER_FAMILY));
    // add delete of the same column
    ThemisDelete delete = new ThemisDelete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER);
    transaction.delete(TABLENAME, delete);
    Assert.assertEquals(2, mutationCache.size());
    Assert.assertTrue(mutationCache.hasMutation(COLUMN));
    Assert.assertTrue(mutationCache.hasMutation(COLUMN_WITH_ANOTHER_FAMILY));
    // another table with columns
    ThemisPut pPut = getThemisPut(COLUMN);
    pPut.add(COLUMN_WITH_ANOTHER_FAMILY.getFamily(), COLUMN_WITH_ANOTHER_FAMILY.getQualifier(), VALUE);
    transaction.put(ANOTHER_TABLENAME, pPut);
    Assert.assertEquals(4, transaction.mutationCache.size());
    Assert.assertTrue(mutationCache.hasMutation(COLUMN));
    Assert.assertTrue(mutationCache.hasMutation(COLUMN_WITH_ANOTHER_FAMILY));
    Assert.assertTrue(mutationCache.hasMutation(COLUMN_WITH_ANOTHER_TABLE));
    ColumnCoordinate expectColumn = new ColumnCoordinate(ANOTHER_TABLENAME, ROW, ANOTHER_FAMILY, QUALIFIER);
    Assert.assertTrue(mutationCache.hasMutation(expectColumn));
  }
  
  @Test
  public void testSelectPrimaryAndSecondaries() throws IOException {
    applyTransactionMutations();
    ThemisPut put = new ThemisPut(ANOTHER_ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    transaction.put(ANOTHER_TABLENAME, put);
    put = new ThemisPut(ANOTHER_ROW);
    put.add(ANOTHER_FAMILY, QUALIFIER, VALUE);
    transaction.put(ANOTHER_TABLENAME, put);
    transaction.selectPrimaryAndSecondaries();
    ColumnCoordinate expectPrimary = new ColumnCoordinate(ANOTHER_TABLENAME, ANOTHER_ROW, ANOTHER_FAMILY, QUALIFIER);
    Assert.assertEquals(expectPrimary, transaction.primary);
    Assert.assertEquals(2, transaction.primaryRow.size());
    Assert.assertArrayEquals(ANOTHER_ROW, transaction.primaryRow.getRow());
    Assert.assertTrue(transaction.primaryRow.hasMutation(ANOTHER_FAMILY, QUALIFIER));
    Assert.assertTrue(transaction.primaryRow.hasMutation(FAMILY, QUALIFIER));
    
    Assert.assertArrayEquals(ANOTHER_TABLENAME, transaction.secondaryRows.get(0).getFirst());
    Assert.assertArrayEquals(ROW, transaction.secondaryRows.get(0).getSecond().getRow());
    Assert.assertEquals(1, transaction.secondaryRows.get(0).getSecond().size());
    Assert.assertTrue(transaction.secondaryRows.get(0).getSecond().hasMutation(FAMILY, QUALIFIER));
    
    Assert.assertArrayEquals(TABLENAME, transaction.secondaryRows.get(1).getFirst());
    Assert.assertArrayEquals(ANOTHER_ROW, transaction.secondaryRows.get(1).getSecond().getRow());
    Assert.assertEquals(1, transaction.secondaryRows.get(1).getSecond().size());
    Assert.assertTrue(transaction.secondaryRows.get(1).getSecond().hasMutation(FAMILY, QUALIFIER));
    
    Assert.assertArrayEquals(TABLENAME, transaction.secondaryRows.get(2).getFirst());
    Assert.assertArrayEquals(ROW, transaction.secondaryRows.get(2).getSecond().getRow());
    Assert.assertEquals(3, transaction.secondaryRows.get(2).getSecond().size());
    Assert.assertTrue(transaction.secondaryRows.get(2).getSecond().hasMutation(FAMILY, QUALIFIER));
    Assert.assertTrue(transaction.secondaryRows.get(2).getSecond().hasMutation(ANOTHER_FAMILY, QUALIFIER));
    Assert.assertTrue(transaction.secondaryRows.get(2).getSecond().hasMutation(FAMILY, ANOTHER_QUALIFIER));
    
    // test null mutations
    createTransactionWithMock();
    try {
      transaction.selectPrimaryAndSecondaries();
      Assert.fail();
    } catch (IOException e) {}
    
    // test single mutations
    createTransactionWithMock();
    put = new ThemisPut(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    transaction.put(TABLENAME, put);
    transaction.selectPrimaryAndSecondaries();
    Assert.assertEquals(1, transaction.primaryRow.size());
    Assert.assertTrue(transaction.primaryRow.hasMutation(FAMILY, QUALIFIER));
    Assert.assertEquals(0, transaction.secondaryRows.size());
  }
  
  @Test
  public void testConstructLock() throws IOException {
    preparePrewrite();
    PrimaryLock primaryLock = transaction.constructPrimaryLock();
    Assert.assertEquals(4, primaryLock.getSecondaryColumns().size());
    Assert.assertEquals(prewriteTs, primaryLock.getTimestamp());
    
    for (ColumnCoordinate secondary : transaction.secondaries) {
      SecondaryLock secondaryLock = transaction.constructSecondaryLock(getColumnType(secondary));
      Assert.assertEquals(transaction.primary, secondaryLock.getPrimaryColumn());
      Assert.assertEquals(prewriteTs, secondaryLock.getTimestamp());
    }
  }
  
  @Test
  public void testPrewritePrimary() throws IOException {
    preparePrewrite();
    transaction.prewritePrimary();
    checkPrewriteRowSuccess(transaction.primary.getTableName(), transaction.primaryRow);
  }

  @Test
  public void testPrewriteSecondariesSuccess() throws IOException {
    preparePrewrite();
    transaction.prewriteSecondaries();
    checkPrewriteSecondariesSuccess();
  }
  
  @Test
  public void testPrewriteFailDueToNewerWrite() throws IOException {
    ColumnCoordinate conflictColumn = COLUMN_WITH_ANOTHER_ROW;
    preparePrewrite();
    writePutColumn(conflictColumn, prewriteTs + 1, commitTs + 1);
    transaction.prewritePrimary();
    try {
      transaction.prewriteSecondaries();
      Assert.fail();
    } catch (WriteConflictException e) {
      checkTransactionRollback();
    }
  }
  
  @Test
  public void testPrewriteFailDueToLockConflict() throws IOException {
    ColumnCoordinate conflictColumn = COLUMN_WITH_ANOTHER_ROW;
    writeLockAndData(conflictColumn, commitTs + 1);
    conf.setInt(TransactionConstant.THEMIS_RETRY_COUNT, 0);
    preparePrewrite();
    transaction.prewritePrimary();
    try {
      transaction.prewriteSecondaries();
      Assert.fail();
    } catch (LockConflictException e) {
      checkTransactionRollback();
    }
  }
  
  @Test
  public void testCommitPrimary() throws IOException {
    // commit primary success
    prepareCommit();
    transaction.commitPrimary();
    checkCommitRowSuccess(transaction.primary.getTableName(), transaction.primaryRow);
    
    // primary lock erased, commit primary fail
    deleteOldDataAndUpdateTs();
    prepareCommit();
    eraseLock(COLUMN, prewriteTs);
    try {
      transaction.commitPrimary();
      Assert.fail();
    } catch (LockCleanedException e) {
      Assert.assertNull(readPut(COLUMN));
      checkSecondariesRollback();
    }
    
    // other ioexception, commit primary fail
    deleteOldDataAndUpdateTs();
    prepareCommit();
    HBaseAdmin admin = new HBaseAdmin(connection.getConfiguration());
    admin.disableTable(TABLENAME);
    try {
      transaction.commitPrimary();
      Assert.fail();
    } catch (IOException e) {
      Assert.assertFalse(e instanceof LockCleanedException);
      admin.enableTable(TABLENAME);
      checkPrewriteRowSuccess(transaction.primary.getTableName(), transaction.primaryRow);
      checkPrewriteSecondariesSuccess();
    } finally {
      admin.close();
    }
  }

  @Test
  public void testCommitSecondaries() throws IOException {
    // commit secondary success
    prepareCommit();
    transaction.commitSecondaries();
    checkCommitSecondaryRowsSuccess();
    // secondary lock has been removed by commit
    deleteOldDataAndUpdateTs();
    prepareCommit();
    eraseLock(COLUMN_WITH_ANOTHER_TABLE, prewriteTs);
    transaction.commitSecondaries();
    checkCommitSecondaryRowsSuccess();
    // commit one secondary lock fail
    deleteOldDataAndUpdateTs();
    prepareCommit();
    HBaseAdmin admin = new HBaseAdmin(connection.getConfiguration());
    admin.disableTable(ANOTHER_TABLENAME);
    transaction.commitSecondaries();
    admin.enableTable(ANOTHER_TABLENAME);
    for (Pair<byte[], RowMutation> secondaryRow : transaction.secondaryRows) {
      RowMutation rowMutation = secondaryRow.getSecond();
      for (Column column : rowMutation.getColumns()) {
        ColumnCoordinate c = new ColumnCoordinate(secondaryRow.getFirst(), rowMutation.getRow(), column); 
        if (COLUMN_WITH_ANOTHER_TABLE.equals(c)) {
          checkPrewriteColumnSuccess(c);
        } else {
          checkCommitColumnSuccess(c);
        }
      }
    }
    admin.close();
  }

  @Test
  public void testTransactionSuccess() throws IOException {
    applyMutations(TRANSACTION_COLUMNS);
    transaction.commit();
    checkTransactionCommitSuccess();
    
    // test single-row transaction
    deleteOldDataAndUpdateTs();
    createTransactionWithMock();
    applyMutations(new ColumnCoordinate[]{COLUMN, COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER});
    mockTimestamp(commitTs);
    transaction.commit();
    Assert.assertEquals(0, transaction.secondaryRows.size());
    checkCommitRowSuccess(TABLENAME, transaction.primaryRow);
  }
  
  @Test
  public void testPutAndDeleteToTheSameColumn() throws IOException {
    ThemisPut put = new ThemisPut(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    transaction.put(TABLENAME, put);
    ThemisDelete delete = new ThemisDelete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER);
    transaction.delete(TABLENAME, delete);
    mockTimestamp(commitTs);
    transaction.commit();
    Result result = getTable(TABLENAME).get(new Get(ROW));
    Assert.assertEquals(1, result.size());
    Column expect = ColumnUtil.getDeleteColumn(COLUMN);
    KeyValue kv = result.list().get(0);
    Column actual = new Column(kv.getFamily(), kv.getQualifier());
    Assert.assertTrue(expect.equals(actual));
  }
}
