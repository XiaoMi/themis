package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestTransactionSingleRowWrite extends ClientTestBase {
  private boolean enableSingleRowWrite;
  
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    createTransactionWithMock();
    enableSingleRowWrite = conf.getBoolean(TransactionConstant.ENABLE_SINGLE_ROW_WRITE, false);
    conf.setBoolean(TransactionConstant.ENABLE_SINGLE_ROW_WRITE, true);
  }
  
  @After
  public void tearDown() throws IOException {
    conf.setBoolean(TransactionConstant.ENABLE_SINGLE_ROW_WRITE, enableSingleRowWrite);
  }
  
  @Test
  public void testPrewritePrimary() throws IOException {
    preparePrewrite(true);
    transaction.prewritePrimary();
    checkPrewriteRowSuccess(transaction.primary.getTableName(), transaction.primaryRow, true);
  }
  
  @Test
  public void testCommitPrimary() throws IOException {
    // commit primary success
    prepareCommit(true);
    transaction.commitPrimary();
    checkCommitRowSuccess(transaction.primary.getTableName(), transaction.primaryRow);
    
    // primary lock erased, commit primary fail
    deleteOldDataAndUpdateTs();
    prepareCommit(true);
    eraseLock(COLUMN, prewriteTs);
    try {
      transaction.commitPrimary();
      Assert.fail();
    } catch (LockCleanedException e) {
      checkRollbackForSingleRow();
    }    
  }
}
