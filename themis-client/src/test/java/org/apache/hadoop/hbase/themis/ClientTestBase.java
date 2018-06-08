package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.themis.lockcleaner.WorkerRegister;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
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
    Mockito.when(mockTimestampOracle.getRequestIdWithTimestamp()).thenReturn(new Pair<Long, Long>(timestamp, timestamp));

  }
  
  public static KeyValue getKeyValue(ColumnCoordinate c, Type type, long ts) {
    return new KeyValue(c.getRow(), c.getFamily(), c.getQualifier(), ts, type, VALUE);
  }
  
  protected ThemisGet getThemisGet(ColumnCoordinate c) throws IOException {
    return new ThemisGet(c.getRow()).addColumn(c.getFamily(), c.getQualifier());
  }
  
  protected void checkReadColumnResultWithTs(Result result, ColumnCoordinate columnCoordinate, long ts) {
    Assert.assertEquals(1, result.size());
    KeyValue kv = result.list().get(0);
    Assert.assertArrayEquals(columnCoordinate.getRow(), kv.getRow());
    Assert.assertArrayEquals(columnCoordinate.getFamily(), kv.getFamily());
    Assert.assertArrayEquals(columnCoordinate.getQualifier(), kv.getQualifier());
    Assert.assertEquals(ts, kv.getTimestamp());
    Assert.assertArrayEquals(VALUE, kv.getValue());
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

  protected void preparePrewrite(boolean singleRowTransaction, boolean addAuxiliary) throws IOException {
    createTransactionWithMock();
    preparePrewriteWithoutCreateTransaction(singleRowTransaction, addAuxiliary);
  }
  
  protected void preparePrewriteWithoutCreateTransaction(boolean singleRowTransaction)
      throws IOException {
    preparePrewriteWithoutCreateTransaction(singleRowTransaction, false);
  }

  protected void preparePrewriteWithoutCreateTransaction(boolean singleRowTransaction, boolean addAuxiliary)
      throws IOException {
    if (singleRowTransaction) {
      applySingleRowTransactionMutations();
    } else {
      applyTransactionMutations();
    }
    transaction.primary = COLUMN;
    // auxiliary column is in primary row
    if (addAuxiliary) {
      ThemisPut put = new ThemisPut(COLUMN.getRow());
      put.add(ColumnUtil.getAuxiliaryFamilyBytes(), ColumnUtil.getAuxiliaryQualifierBytes(),
          AUXILIARY_VALUE);
      transaction.put(COLUMN.getTableName(), put);
    }
    transaction.selectPrimaryAndSecondaries();
  }
  
  protected void checkPrewriteSecondariesSuccess() throws IOException {
    for (Pair<byte[], RowMutation> secondaryRow : transaction.secondaryRows) {
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
    for (Pair<byte[], RowMutation> secondaryRow : transaction.secondaryRows) {
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
    Assert.assertNull(scanner.next());
    scanner.close();
  }
}