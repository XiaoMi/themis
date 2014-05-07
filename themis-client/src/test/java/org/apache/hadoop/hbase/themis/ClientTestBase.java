package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.themis.lockcleaner.WallClock;
import org.apache.hadoop.hbase.themis.lockcleaner.WorkerRegister;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.mockito.Mockito;

public class ClientTestBase extends TransactionTestBase {
  protected WorkerRegister mockRegister;
  protected WallClock mockClock;
  protected BaseTimestampOracle mockTimestampOracle;
  protected Transaction transaction;
  
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    mockRegister = Mockito.mock(WorkerRegister.class);
    mockClock = Mockito.mock(WallClock.class);
    Mockito.when(mockRegister.getClientAddress()).thenReturn(CLIENT_TEST_ADDRESS);
  }
  
  protected void createTransactionWithMock() throws IOException {
    mockTimestampOracle = Mockito.mock(BaseTimestampOracle.class);
    mockTimestamp(prewriteTs);
    Mockito.when(mockClock.getWallTime()).thenReturn(prewriteTs);
    transaction = new Transaction(conf, connection, mockTimestampOracle, mockClock, mockRegister);
  }
  
  protected void mockTimestamp(long timestamp) throws IOException {
    Mockito.when(mockTimestampOracle.getRequestIdWithTimestamp()).thenReturn(new Pair<Long, Long>(timestamp, timestamp));

  }
  
  public static KeyValue getKeyValue(ColumnCoordinate c, Type type, long ts) {
    return new KeyValue(c.getRow(), c.getFamily(), c.getQualifier(), ts, type, VALUE);
  }
  
  protected void checkCommitSecondariesSuccess() throws IOException {
    for (ColumnCoordinate columnCoordinate : SECONDARY_COLUMNS) {
      checkCommitColumnSuccess(columnCoordinate);
    }
  }
  
  protected void checkSecondariesRollback() throws IOException {
    for (ColumnCoordinate columnCoordinate : SECONDARY_COLUMNS) {
      checkColumnRollback(columnCoordinate);
    }
  }
  
  protected void checkColumnRollback(ColumnCoordinate columnCoordinate) throws IOException {
    Assert.assertNull(readLockBytes(columnCoordinate));
    Assert.assertNull(readPut(columnCoordinate));
    Assert.assertNull(readDelete(columnCoordinate));
  }
  
  protected void checkTransactionRollback() throws IOException {
    for (ColumnCoordinate columnCoordinate : TRANSACTION_COLUMNS) {
      checkColumnRollback(columnCoordinate);
    }
  }
  
  public void checkTransactionCommitSuccess() throws IOException {
    checkCommitRowSuccess(TABLENAME, PRIMARY_ROW);
    checkCommitSecondariesSuccess();
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
  
  protected void preparePrewrite() throws IOException {
    createTransactionWithMock();
    applyTransactionMutations();
    transaction.wallTime = wallTime;
    transaction.primary = COLUMN;
    transaction.selectPrimaryAndSecondaries();
  }
  
  protected void checkPrewriteSecondariesSuccess() throws IOException {
    for (Pair<byte[], RowMutation> secondaryRow : transaction.secondaryRows) {
      checkPrewriteRowSuccess(secondaryRow.getFirst(), secondaryRow.getSecond());
    }
  }
  
  protected void prepareCommit() throws IOException {
    preparePrewrite();
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
}