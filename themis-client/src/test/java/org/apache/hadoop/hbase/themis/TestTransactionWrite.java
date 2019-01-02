package org.apache.hadoop.hbase.themis;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
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
    assertEquals(1, mutationCache.size());
    assertTrue(mutationCache.hasMutation(COLUMN));
    // add another put mutation into the same column
    transaction.put(TABLENAME, getThemisPut(COLUMN, ANOTHER_VALUE));
    mutationCache = transaction.mutationCache;
    assertEquals(1, mutationCache.size());
    assertTrue(mutationCache.hasMutation(COLUMN));
    assertTrue(Bytes.equals(ANOTHER_VALUE, mutationCache.getMutation(COLUMN).getSecond()));
    // put contains different columns
    transaction.put(TABLENAME, getThemisPut(COLUMN_WITH_ANOTHER_FAMILY));
    mutationCache = transaction.mutationCache;
    assertEquals(2, mutationCache.size());
    assertTrue(mutationCache.hasMutation(COLUMN));
    assertTrue(mutationCache.hasMutation(COLUMN_WITH_ANOTHER_FAMILY));
    // add delete of the same column
    ThemisDelete delete = new ThemisDelete(ROW);
    delete.deleteColumn(FAMILY, QUALIFIER);
    transaction.delete(TABLENAME, delete);
    assertEquals(2, mutationCache.size());
    assertTrue(mutationCache.hasMutation(COLUMN));
    assertTrue(mutationCache.hasMutation(COLUMN_WITH_ANOTHER_FAMILY));
    // another table with columns
    ThemisPut pPut = getThemisPut(COLUMN);
    pPut.add(COLUMN_WITH_ANOTHER_FAMILY.getFamily(), COLUMN_WITH_ANOTHER_FAMILY.getQualifier(),
      VALUE);
    transaction.put(ANOTHER_TABLENAME, pPut);
    assertEquals(4, transaction.mutationCache.size());
    assertTrue(mutationCache.hasMutation(COLUMN));
    assertTrue(mutationCache.hasMutation(COLUMN_WITH_ANOTHER_FAMILY));
    assertTrue(mutationCache.hasMutation(COLUMN_WITH_ANOTHER_TABLE));
    ColumnCoordinate expectColumn =
      new ColumnCoordinate(ANOTHER_TABLENAME, ROW, ANOTHER_FAMILY, QUALIFIER);
    assertTrue(mutationCache.hasMutation(expectColumn));
  }

  @Test
  public void testSelectPrimaryAndSecondaries() throws IOException {
    // test null mutations
    createTransactionWithMock();
    try {
      transaction.selectPrimaryAndSecondaries();
      fail();
    } catch (IOException e) {
    }

    boolean[] isAddAuxiliaries = { true, false };
    for (boolean isAddAuxiliary : isAddAuxiliaries) {
      createTransactionWithMock();
      applyTransactionMutations();
      ThemisPut put = new ThemisPut(ANOTHER_ROW);
      put.add(FAMILY, QUALIFIER, VALUE);
      transaction.put(ANOTHER_TABLENAME, put);
      put = new ThemisPut(ANOTHER_ROW);
      put.add(ANOTHER_FAMILY, QUALIFIER, VALUE);
      if (isAddAuxiliary) {
        put.add(ColumnUtil.AUXILIARY_FAMILY_BYTES, ColumnUtil.AUXILIARY_QUALIFIER_BYTES,
          AUXILIARY_VALUE);
      }
      transaction.put(ANOTHER_TABLENAME, put);
      transaction.selectPrimaryAndSecondaries();
      ColumnCoordinate expectPrimary =
        new ColumnCoordinate(ANOTHER_TABLENAME, ANOTHER_ROW, ANOTHER_FAMILY, QUALIFIER);
      assertEquals(expectPrimary, transaction.primary);
      assertEquals(2, transaction.primaryRow.size());
      assertArrayEquals(ANOTHER_ROW, transaction.primaryRow.getRow());
      assertTrue(transaction.primaryRow.hasMutation(ANOTHER_FAMILY, QUALIFIER));
      assertTrue(transaction.primaryRow.hasMutation(FAMILY, QUALIFIER));
      if (isAddAuxiliary) {
        assertTrue(!transaction.primaryRow
            .hasMutation(ColumnUtil.AUXILIARY_FAMILY_BYTES, ColumnUtil.AUXILIARY_QUALIFIER_BYTES));
        assertArrayEquals(transaction.auxiliaryColumnMutation.getValue(), AUXILIARY_VALUE);
      }
      assertEquals(ANOTHER_TABLENAME, transaction.secondaryRows.get(0).getFirst());
      assertArrayEquals(ROW, transaction.secondaryRows.get(0).getSecond().getRow());
      assertEquals(1, transaction.secondaryRows.get(0).getSecond().size());
      Assert
        .assertTrue(transaction.secondaryRows.get(0).getSecond().hasMutation(FAMILY, QUALIFIER));

      assertEquals(TABLENAME, transaction.secondaryRows.get(1).getFirst());
      assertArrayEquals(ANOTHER_ROW, transaction.secondaryRows.get(1).getSecond().getRow());
      assertEquals(1, transaction.secondaryRows.get(1).getSecond().size());
      Assert
        .assertTrue(transaction.secondaryRows.get(1).getSecond().hasMutation(FAMILY, QUALIFIER));

      assertEquals(TABLENAME, transaction.secondaryRows.get(2).getFirst());
      assertArrayEquals(ROW, transaction.secondaryRows.get(2).getSecond().getRow());
      assertEquals(3, transaction.secondaryRows.get(2).getSecond().size());
      Assert
        .assertTrue(transaction.secondaryRows.get(2).getSecond().hasMutation(FAMILY, QUALIFIER));
      assertTrue(
        transaction.secondaryRows.get(2).getSecond().hasMutation(ANOTHER_FAMILY, QUALIFIER));
      assertTrue(
        transaction.secondaryRows.get(2).getSecond().hasMutation(FAMILY, ANOTHER_QUALIFIER));

      // test single mutations
      createTransactionWithMock();
      put = new ThemisPut(ROW);
      put.add(FAMILY, QUALIFIER, VALUE);
      if (isAddAuxiliary) {
        put.add(ColumnUtil.AUXILIARY_FAMILY_BYTES, ColumnUtil.AUXILIARY_QUALIFIER_BYTES,
          AUXILIARY_VALUE);
      }
      transaction.put(TABLENAME, put);
      transaction.selectPrimaryAndSecondaries();
      assertEquals(1, transaction.primaryRow.size());
      assertTrue(transaction.primaryRow.hasMutation(FAMILY, QUALIFIER));
      if (isAddAuxiliary) {
        assertTrue(!transaction.primaryRow.hasMutation(ColumnUtil.AUXILIARY_FAMILY_BYTES,
          ColumnUtil.AUXILIARY_QUALIFIER_BYTES));
        assertArrayEquals(transaction.auxiliaryColumnMutation.getValue(), AUXILIARY_VALUE);
      }
      assertEquals(0, transaction.secondaryRows.size());
    }
  }

  @Test
  public void testConstructLock() throws IOException {
    preparePrewrite();
    PrimaryLock primaryLock = transaction.constructPrimaryLock();
    assertEquals(4, primaryLock.getSecondaryColumns().size());
    assertEquals(prewriteTs, primaryLock.getTimestamp());

    for (ColumnCoordinate secondary : transaction.secondaries) {
      SecondaryLock secondaryLock = transaction.constructSecondaryLock(getColumnType(secondary));
      assertEquals(transaction.primary, secondaryLock.getPrimaryColumn());
      assertEquals(prewriteTs, secondaryLock.getTimestamp());
    }
  }

  @Test
  public void testPrewritePrimary() throws IOException {
    preparePrewrite();
    transaction.prewritePrimary();
    checkPrewriteRowSuccess(transaction.primary.getTableName(), transaction.primaryRow);
  }

  @Test
  public void testPrewritePrimaryWithAuxiliary() throws IOException {
    preparePrewrite(false, true);
    transaction.prewritePrimary();
    assertNotNull(transaction.auxiliaryColumnMutation);
    ColumnCoordinate auxiliaryCC = new ColumnCoordinate(transaction.primary.getTableName(),
      transaction.primary.getRow(), transaction.auxiliaryColumnMutation);
    checkPrewriteRowSuccess(transaction.primary.getTableName(), transaction.primaryRow,
      auxiliaryCC);
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
      fail();
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
      fail();
    } catch (LockConflictException e) {
      checkTransactionRollback();
    }
  }

  @Test
  public void testCommitPrimaryWithAuxiliary() throws IOException {
    prepareCommit(false, true);
    transaction.commitPrimary();
    checkCommitRowSuccess(transaction.primary.getTableName(), transaction.primaryRow);
    ColumnCoordinate auxiliaryCC = new ColumnCoordinate(transaction.primary.getTableName(),
      transaction.primary.getRow(), transaction.auxiliaryColumnMutation);
    checkCommitAuxiliaryColumnSuccess(auxiliaryCC);

    // primary lock erased, commit primary fail
    deleteOldDataAndUpdateTs();
    prepareCommit(false, true);
    eraseLock(COLUMN, prewriteTs);
    try {
      transaction.commitPrimary();
      fail();
    } catch (LockCleanedException e) {
      assertNull(readPut(COLUMN));
      checkSecondariesRollback();
      auxiliaryCC = new ColumnCoordinate(transaction.primary.getTableName(),
        transaction.primary.getRow(), transaction.auxiliaryColumnMutation);
      assertNull(readDataValue(auxiliaryCC, commitTs));
    }

    // other ioexception, commit primary fail
    deleteOldDataAndUpdateTs();
    prepareCommit(false, true);
    try (Admin admin = connection.getAdmin()) {
      admin.disableTable(TABLENAME);
      try {
        transaction.commitPrimary();
        fail();
      } catch (IOException e) {
        assertFalse(e instanceof LockCleanedException);
        admin.enableTable(TABLENAME);
        checkPrewriteRowSuccess(transaction.primary.getTableName(), transaction.primaryRow);
        checkPrewriteSecondariesSuccess();
        auxiliaryCC = new ColumnCoordinate(transaction.primary.getTableName(),
          transaction.primary.getRow(), transaction.auxiliaryColumnMutation);
        assertNull(readDataValue(auxiliaryCC, commitTs));
      }
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
      fail();
    } catch (LockCleanedException e) {
      assertNull(readPut(COLUMN));
      checkSecondariesRollback();
    }

    // other ioexception, commit primary fail
    deleteOldDataAndUpdateTs();
    prepareCommit();
    try (Admin admin = connection.getAdmin()) {
      admin.disableTable(TABLENAME);
      try {
        transaction.commitPrimary();
        fail();
      } catch (IOException e) {
        assertFalse(e instanceof LockCleanedException);
        admin.enableTable(TABLENAME);
        checkPrewriteRowSuccess(transaction.primary.getTableName(), transaction.primaryRow);
        checkPrewriteSecondariesSuccess();
      }
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
    try (Admin admin = connection.getAdmin()) {
      admin.disableTable(ANOTHER_TABLENAME);
      transaction.commitSecondaries();
      admin.enableTable(ANOTHER_TABLENAME);
    }
    for (Pair<TableName, RowMutation> secondaryRow : transaction.secondaryRows) {
      RowMutation rowMutation = secondaryRow.getSecond();
      for (Column column : rowMutation.getColumns()) {
        ColumnCoordinate c =
          new ColumnCoordinate(secondaryRow.getFirst(), rowMutation.getRow(), column);
        if (COLUMN_WITH_ANOTHER_TABLE.equals(c)) {
          checkPrewriteColumnSuccess(c);
        } else {
          checkCommitColumnSuccess(c);
        }
      }
    }
  }

  protected Pair<TableName, ThemisPut> getAuxiliaryRequest() throws IOException {
    TableName tableName = null;
    byte[] row = null;
    if (transaction.primary != null) {
      tableName = transaction.primary.getTableName();
      row = transaction.primary.getRow();
    } else {
      Map.Entry<TableName, Map<byte[], RowMutation>> primaryEntry =
        transaction.getMutations().getMutations().iterator().next();
      tableName = primaryEntry.getKey();
      row = primaryEntry.getValue().keySet().iterator().next();
    }
    ThemisPut put = new ThemisPut(row);
    put.add(ColumnUtil.AUXILIARY_FAMILY_BYTES, ColumnUtil.AUXILIARY_QUALIFIER_BYTES,
      AUXILIARY_VALUE);
    return new Pair<>(tableName, put);
  }

  @Test
  public void testTransactionSuccess() throws IOException {
    boolean[] isAddAuxiliaries = { false, true };
    for (boolean isAddAuxiliary : isAddAuxiliaries) {
      deleteOldDataAndUpdateTs();
      createTransactionWithMock();
      applyMutations(TRANSACTION_COLUMNS);
      if (isAddAuxiliary) {
        Pair<TableName, ThemisPut> auxiliaryRequest = getAuxiliaryRequest();
        transaction.put(auxiliaryRequest.getFirst(), auxiliaryRequest.getSecond());
      }
      transaction.commit();
      checkTransactionCommitSuccess();
      if (isAddAuxiliary) {
        ColumnCoordinate auxiliaryCC = new ColumnCoordinate(transaction.primary.getTableName(),
          transaction.primary.getRow(), transaction.auxiliaryColumnMutation);
        checkCommitAuxiliaryColumnSuccess(auxiliaryCC);
      }

      // test single-row transaction
      deleteOldDataAndUpdateTs();
      createTransactionWithMock();
      applyMutations(new ColumnCoordinate[] { COLUMN, COLUMN_WITH_ANOTHER_FAMILY,
        COLUMN_WITH_ANOTHER_QUALIFIER });
      mockTimestamp(commitTs);
      if (isAddAuxiliary) {
        Pair<TableName, ThemisPut> auxiliaryRequest = getAuxiliaryRequest();
        transaction.put(auxiliaryRequest.getFirst(), auxiliaryRequest.getSecond());
      }
      transaction.commit();
      assertEquals(0, transaction.secondaryRows.size());
      checkCommitRowSuccess(TABLENAME, transaction.primaryRow);
      if (isAddAuxiliary) {
        ColumnCoordinate auxiliaryCC = new ColumnCoordinate(transaction.primary.getTableName(),
          transaction.primary.getRow(), transaction.auxiliaryColumnMutation);
        checkCommitAuxiliaryColumnSuccess(auxiliaryCC);
      }
    }
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
    assertEquals(1, result.size());
    Column expect = ColumnUtil.getDeleteColumn(COLUMN);
    Cell kv = result.rawCells()[0];
    Column actual = new Column(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv));
    assertTrue(expect.equals(actual));
  }
}
