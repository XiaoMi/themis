package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.themis.ConcurrentRowCallables.TableAndRow;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.exception.MultiRowExceptions;
import org.apache.hadoop.hbase.themis.exception.WriteConflictException;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

public class TestTransactionWithConcurrentCallable extends ClientTestBase {
  private ExecutorService threadPool;

  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    createTransactionWithMock();
  }

  protected void createTransactionWithMock() throws IOException {
    super.createTransactionWithMock();
    conf.setBoolean(TransactionConstant.THEMIS_ENABLE_CONCURRENT_RPC, true);
    transaction = new Transaction(conf, connection, mockTimestampOracle, mockRegister);
    ArrayBlockingQueue<Runnable> requestQueue = new ArrayBlockingQueue<Runnable>(100);
    this.threadPool = new ThreadPoolExecutor(1, 10, 10, TimeUnit.SECONDS, requestQueue);
    Transaction.setThreadPool(this.threadPool);
    conf.setBoolean(TransactionConstant.THEMIS_ENABLE_CONCURRENT_RPC, false);
  }

  protected void createThreadPoolForToRejectRequest() throws IOException {
    SynchronousQueue<Runnable> requestQueue = new SynchronousQueue<Runnable>();
    this.threadPool = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS, requestQueue);
  }

  @Test
  public void testConcurrentPrewriteSuccess() throws IOException {
    preparePrewrite();
    transaction.concurrentPrewriteSecondaries();
    checkPrewriteSecondariesSuccess();
  }

  protected void waitForThreadPoolTerminated() throws IOException {
    try {
      threadPool.shutdown();
      if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
        throw new IOException("threadPool not terminated");
      }
    } catch (Throwable e) {
      throw new IOException(e);
    }
  }

  @Test
  public void testConcurrentPrewriteFailDueToNewerWrite() throws IOException {
    ColumnCoordinate conflictColumn = COLUMN_WITH_ANOTHER_ROW;
    preparePrewrite();
    writePutColumn(conflictColumn, prewriteTs + 1, commitTs + 1);
    try {
      transaction.concurrentPrewriteSecondaries();
      Assert.fail();
    } catch (MultiRowExceptions e) {
      waitForThreadPoolTerminated();
      checkTransactionRollback();
      Assert.assertEquals(1, e.getExceptions().size());
      TableAndRow tableAndRow =
        new TableAndRow(conflictColumn.getTableName(), conflictColumn.getRow());
      IOException exception = e.getExceptions().get(tableAndRow);
      Assert.assertTrue(exception.getCause().getCause() instanceof WriteConflictException);
    }
  }

  @Test
  public void testConcurrentPrewriteFailDueToLockConflict() throws IOException {
    ColumnCoordinate conflictColumn = COLUMN_WITH_ANOTHER_ROW;
    writeLockAndData(conflictColumn, commitTs + 1);
    conf.setInt(TransactionConstant.THEMIS_RETRY_COUNT, 0);
    preparePrewrite();
    try {
      transaction.concurrentPrewriteSecondaries();
      Assert.fail();
    } catch (MultiRowExceptions e) {
      waitForThreadPoolTerminated();
      checkTransactionRollback();
      Assert.assertEquals(1, e.getExceptions().size());
      TableAndRow tableAndRow =
        new TableAndRow(conflictColumn.getTableName(), conflictColumn.getRow());
      IOException exception = e.getExceptions().get(tableAndRow);
      Assert.assertTrue(exception.getCause().getCause() instanceof LockConflictException);
    }
  }

  @Test
  public void testConcurrentPrewriteWithRequestRejected() throws IOException {
    preparePrewrite();
    createThreadPoolForToRejectRequest();
    Transaction.setThreadPool(this.threadPool);
    try {
      transaction.concurrentPrewriteSecondaries();
      Assert.fail();
    } catch (MultiRowExceptions e) {
      waitForThreadPoolTerminated();
      // the queue might be filled, can not ensure rollback successfully
      Assert.assertEquals(1, e.getExceptions().size());
      TableAndRow tableAndRow = new TableAndRow(TABLENAME, ANOTHER_ROW);
      Assert.assertTrue(
        e.getExceptions().get(tableAndRow).getCause() instanceof RejectedExecutionException);
    }
  }

  @Test
  public void testConcurrentCommitSecondaries() throws IOException {
    // commit secondary success
    prepareCommit();
    transaction.concurrentCommitSecondaries();
    checkCommitSecondaryRowsSuccess();
    // secondary lock has been removed by commit
    deleteOldDataAndUpdateTs();
    prepareCommit();
    eraseLock(COLUMN_WITH_ANOTHER_TABLE, prewriteTs);
    transaction.concurrentCommitSecondaries();
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

  @Test
  public void testConcurrentConcurrentCommitWithRequestRejected() throws IOException {
    prepareCommit();
    createThreadPoolForToRejectRequest();
    Transaction.setThreadPool(this.threadPool);
    try {
      transaction.concurrentCommitSecondaries();
    } catch (MultiRowExceptions e) {
      waitForThreadPoolTerminated();
      Assert.assertEquals(1, e.getExceptions().size());
      TableAndRow tableAndRow = new TableAndRow(TABLENAME, ANOTHER_ROW);
      Assert.assertTrue(
        e.getExceptions().get(tableAndRow).getCause() instanceof RejectedExecutionException);
    }
  }

  @Test
  public void testTransactionSuccess() throws IOException {
    applyMutations(TRANSACTION_COLUMNS);
    transaction.commit();
    checkTransactionCommitSuccess();

    // test single-row transaction
    deleteOldDataAndUpdateTs();
    createTransactionWithMock();
    applyMutations(
      new ColumnCoordinate[] { COLUMN, COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER });
    mockTimestamp(commitTs);
    transaction.commit();
    Assert.assertEquals(0, transaction.secondaryRows.size());
    checkCommitRowSuccess(TABLENAME, transaction.primaryRow);
  }
}
