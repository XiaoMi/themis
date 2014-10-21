package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.junit.Assert;
import org.junit.Test;

public class TestTransactionRead extends ClientTestBase {
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    createTransactionWithMock();
  }
  
  @Test
  public void testGetOneColumn() throws IOException {
    nextTransactionTs();
    createTransactionWithMock();
    // empty result
    Assert.assertTrue(transaction.get(TABLENAME, getThemisGet(COLUMN)).isEmpty());
    // lock column and write column after startTs
    writeLockAndData(COLUMN, prewriteTs + 1);
    writePutColumn(COLUMN, prewriteTs + 1, prewriteTs + 2);
    Assert.assertTrue(transaction.get(TABLENAME, getThemisGet(COLUMN)).isEmpty());
    // lock column after startTs, write column before startTs, no data column, will throw exception
    writePutColumn(COLUMN, prewriteTs - 2, prewriteTs - 1);
    Assert.assertTrue(transaction.get(TABLENAME, getThemisGet(COLUMN)).isEmpty());
    // write data column with mismatching timestamp
    writeData(COLUMN, prewriteTs - 3);
    Assert.assertTrue(transaction.get(TABLENAME, getThemisGet(COLUMN)).isEmpty());
    // lock column after startTs, write column before startTs, has data column
    writeData(COLUMN, prewriteTs - 2);
    Result result = transaction.get(TABLENAME, getThemisGet(COLUMN));
    checkReadColumnResultWithTs(result, COLUMN, prewriteTs - 2);
  }
  
  @Test
  public void testGetEntireRow() throws IOException {
    commitTestTransaction();
    nextTransactionTs();
    createTransactionWithMock();
    Result result = transaction.get(TABLENAME, new ThemisGet(ROW));
    Assert.assertEquals(2, result.size());
    checkResultKvColumn(COLUMN_WITH_ANOTHER_FAMILY, result.list().get(0));
    checkResultKvColumn(COLUMN, result.list().get(1));
  }

  @Test
  public void testGetOneColumnWithDelete() throws IOException {
    nextTransactionTs();
    createTransactionWithMock();
    // delete after put and delete before prewriteTs
    writeDeleteAfterPut(COLUMN, prewriteTs - 5, commitTs - 5);
    Assert.assertTrue(transaction.get(TABLENAME, getThemisGet(COLUMN)).isEmpty());
    // delete after put and delete after prewriteTs
    writeDeleteAfterPut(COLUMN, prewriteTs + 1, commitTs + 1);
    Result result = transaction.get(TABLENAME, getThemisGet(COLUMN));
    checkReadColumnResultWithTs(result, COLUMN, prewriteTs - 2);
    // delete before put and put before prewriteTs
    nextTransactionTs();
    createTransactionWithMock();
    writePutAfterDelete(COLUMN, prewriteTs - 5, commitTs - 5);
    result = transaction.get(TABLENAME, getThemisGet(COLUMN));
    checkReadColumnResultWithTs(result, COLUMN, prewriteTs - 5);
    // delete before put and put after prewriteTs
    writePutAfterDelete(COLUMN, prewriteTs + 1, commitTs + 1);
    Assert.assertTrue(transaction.get(TABLENAME, getThemisGet(COLUMN)).isEmpty());
  }

  protected ThemisGet constructGetWithMultiColumns() throws IOException {
    ThemisGet get = new ThemisGet(ROW);
    get.addColumn(COLUMN.getFamily(), COLUMN.getQualifier());
    get.addColumn(COLUMN_WITH_ANOTHER_FAMILY.getFamily(), COLUMN_WITH_ANOTHER_FAMILY.getQualifier());
    get.addColumn(COLUMN_WITH_ANOTHER_QUALIFIER.getFamily(), COLUMN_WITH_ANOTHER_QUALIFIER.getQualifier());
    return get;
  }
  
  @Test
  public void testGetMultiColumns() throws IOException {
    nextTransactionTs();
    createTransactionWithMock();
    // put and delete both before prewriteTs
    writeDeleteAfterPut(COLUMN_WITH_ANOTHER_FAMILY, prewriteTs - 5, commitTs - 5);
    writePutAfterDelete(COLUMN_WITH_ANOTHER_QUALIFIER, prewriteTs - 5, commitTs - 5);
    Result result = transaction.get(TABLENAME, constructGetWithMultiColumns());
    checkReadColumnResultWithTs(result, COLUMN_WITH_ANOTHER_QUALIFIER, prewriteTs - 5);
    // one of the put or delete after prewriteTs
    writeDeleteAfterPut(COLUMN_WITH_ANOTHER_FAMILY, prewriteTs + 1, commitTs + 1);
    writePutAfterDelete(COLUMN_WITH_ANOTHER_QUALIFIER, prewriteTs + 1, commitTs + 1);
    result = transaction.get(TABLENAME, constructGetWithMultiColumns());
    checkReadColumnResultWithTs(result, COLUMN_WITH_ANOTHER_FAMILY, prewriteTs - 2);
  }  
}