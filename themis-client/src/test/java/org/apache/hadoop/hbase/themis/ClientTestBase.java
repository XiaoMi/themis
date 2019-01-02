package org.apache.hadoop.hbase.themis;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.themis.lockcleaner.WorkerRegister;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle;
import org.apache.hadoop.hbase.util.Pair;
import org.mockito.Mockito;

public class ClientTestBase extends TransactionTestBase {
  protected WorkerRegister mockRegister;
  protected BaseTimestampOracle mockTimestampOracle;
  protected Transaction transaction;

  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    conf.setInt(TransactionConstant.THEMIS_RETRY_COUNT, 1);
    mockRegister = Mockito.mock(WorkerRegister.class);
    Mockito.when(mockRegister.getClientAddress()).thenReturn(CLIENT_TEST_ADDRESS);
  }

  protected void createTransactionWithMock() throws IOException {
    mockTimestampOracle = Mockito.mock(BaseTimestampOracle.class);
    mockTimestamp(prewriteTs);
    transaction = new Transaction(conf, connection, mockTimestampOracle, mockRegister);
  }

  protected void createTransactionWithWorkRegister(WorkerRegister reg) throws IOException {
    mockTimestampOracle = Mockito.mock(BaseTimestampOracle.class);
    mockTimestamp(prewriteTs);
    transaction = new Transaction(conf, connection, mockTimestampOracle, reg);
  }

  protected void mockTimestamp(long timestamp) throws IOException {
    Mockito.when(mockTimestampOracle.getRequestIdWithTimestamp())
      .thenReturn(new Pair<Long, Long>(timestamp, timestamp));

  }

  public static Cell getKeyValue(ColumnCoordinate c, Type type, long ts) {
    return CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(c.getRow())
      .setFamily(c.getFamily()).setQualifier(c.getQualifier()).setType(type).setValue(VALUE)
      .build();
  }

  protected ThemisGet getThemisGet(ColumnCoordinate c) throws IOException {
    return new ThemisGet(c.getRow()).addColumn(c.getFamily(), c.getQualifier());
  }

  protected void checkReadColumnResultWithTs(Result result, ColumnCoordinate columnCoordinate,
      long ts) {
    assertEquals(1, result.size());
    Cell kv = result.rawCells()[0];
    assertArrayEquals(columnCoordinate.getRow(), CellUtil.cloneRow(kv));
    assertArrayEquals(columnCoordinate.getFamily(), CellUtil.cloneFamily(kv));
    assertArrayEquals(columnCoordinate.getQualifier(), CellUtil.cloneQualifier(kv));
    assertEquals(ts, kv.getTimestamp());
    assertArrayEquals(VALUE, CellUtil.cloneValue(kv));
  }

  protected ThemisPut getThemisPut(ColumnCoordinate columnCoordinate) throws IOException {
    return getThemisPut(columnCoordinate, VALUE);
  }

  protected ThemisPut getThemisPut(ColumnCoordinate c, byte[] value) throws IOException {
    return new ThemisPut(c.getRow()).add(c.getFamily(), c.getQualifier(), value);
  }

  protected ThemisDelete getThemisDelete(ColumnCoordinate c) throws IOException {
    return new ThemisDelete(c.getRow()).deleteColumn(c.getFamily(), c.getQualifier());
  }

  protected void applyTransactionMutations() throws IOException {
    transaction.put(COLUMN.getTableName(), getThemisPut(COLUMN));
    for (ColumnCoordinate columnCoordinate : SECONDARY_COLUMNS) {
      if (getColumnType(columnCoordinate).equals(Type.Put)) {
        transaction.put(columnCoordinate.getTableName(), getThemisPut(columnCoordinate));
      } else {
        transaction.delete(columnCoordinate.getTableName(), getThemisDelete(columnCoordinate));
      }
    }
  }

  protected void applySingleRowTransactionMutations() throws IOException {
    for (ColumnCoordinate columnCoordinate : PRIMARY_ROW_COLUMNS) {
      if (getColumnType(columnCoordinate).equals(Type.Put)) {
        transaction.put(columnCoordinate.getTableName(), getThemisPut(columnCoordinate));
      } else {
        transaction.delete(columnCoordinate.getTableName(), getThemisDelete(columnCoordinate));
      }
    }
  }

  protected void preparePrewrite() throws IOException {
    preparePrewrite(false);
  }

  protected void preparePrewrite(boolean singleRowTransaction) throws IOException {
    preparePrewrite(singleRowTransaction, false);
  }

  protected void preparePrewrite(boolean singleRowTransaction, boolean addAuxiliary)
      throws IOException {
    createTransactionWithMock();
    preparePrewriteWithoutCreateTransaction(singleRowTransaction, addAuxiliary);
  }

  protected void preparePrewriteWithoutCreateTransaction(boolean singleRowTransaction)
      throws IOException {
    preparePrewriteWithoutCreateTransaction(singleRowTransaction, false);
  }

  protected void preparePrewriteWithoutCreateTransaction(boolean singleRowTransaction,
      boolean addAuxiliary) throws IOException {
    if (singleRowTransaction) {
      applySingleRowTransactionMutations();
    } else {
      applyTransactionMutations();
    }
    transaction.primary = COLUMN;
    // auxiliary column is in primary row
    if (addAuxiliary) {
      ThemisPut put = new ThemisPut(COLUMN.getRow());
      put.add(ColumnUtil.AUXILIARY_FAMILY_BYTES, ColumnUtil.AUXILIARY_QUALIFIER_BYTES,
        AUXILIARY_VALUE);
      transaction.put(COLUMN.getTableName(), put);
    }
    transaction.selectPrimaryAndSecondaries();
  }

  protected void checkPrewriteSecondariesSuccess() throws IOException {
    for (Pair<TableName, RowMutation> secondaryRow : transaction.secondaryRows) {
      checkPrewriteRowSuccess(secondaryRow.getFirst(), secondaryRow.getSecond());
    }
  }

  protected void prepareCommit() throws IOException {
    prepareCommit(false);
  }

  protected void prepareCommit(boolean singleRow) throws IOException {
    prepareCommit(singleRow, false);
  }

  protected void prepareCommit(boolean singleRow, boolean addAuxiliary) throws IOException {
    preparePrewrite(singleRow, addAuxiliary);
    transaction.prewritePrimary();
    transaction.prewriteSecondaries();
    transaction.commitTs = commitTs;
  }

  protected void checkCommitSecondaryRowsSuccess() throws IOException {
    for (Pair<TableName, RowMutation> secondaryRow : transaction.secondaryRows) {
      checkCommitRowSuccess(secondaryRow.getFirst(), secondaryRow.getSecond());
    }
  }

  protected void applyMutations(ColumnCoordinate[] columns) throws IOException {
    mockTimestamp(commitTs);
    for (ColumnCoordinate columnCoordinate : columns) {
      if (getColumnType(columnCoordinate).equals(Type.Put)) {
        ThemisPut put = new ThemisPut(columnCoordinate.getRow());
        put.add(columnCoordinate.getFamily(), columnCoordinate.getQualifier(), VALUE);
        transaction.put(columnCoordinate.getTableName(), put);
      } else {
        ThemisDelete delete = new ThemisDelete(columnCoordinate.getRow());
        delete.deleteColumn(columnCoordinate.getFamily(), columnCoordinate.getQualifier());
        transaction.delete(columnCoordinate.getTableName(), delete);
      }
    }
  }

  protected void prepareScanData(ColumnCoordinate[] columns) throws IOException {
    deleteOldDataAndUpdateTs();
    for (ColumnCoordinate columnCoordinate : columns) {
      writePutAndData(columnCoordinate, prewriteTs, commitTs);
    }
    nextTransactionTs();
    createTransactionWithMock();
  }

  protected void checkAndCloseScanner(ResultScanner scanner) throws IOException {
    assertNull(scanner.next());
    scanner.close();
  }
}