package com.xiaomi.infra.themis.validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.themis.ThemisDelete;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.exception.ThemisException;
import org.apache.hadoop.hbase.themis.exception.WriteConflictException;
import org.apache.hadoop.hbase.util.Bytes;

import com.xiaomi.infra.themis.ReadOnlyTransaction;
import com.xiaomi.infra.themis.TransactionWithFail;
import com.xiaomi.infra.themis.WorkerManager;
import com.xiaomi.infra.themis.checker.ValueChecker;
import com.xiaomi.infra.themis.exception.GeneratedTestException;

public class WriteWorker extends Thread {
  private static Log LOG = LogFactory.getLog(WriteWorker.class);
  private Random random;
  private Configuration conf;
  private int runCount = 0;
  private int failCount = 0;
  private int lockConflictFail = 0;
  private int writeConflictFail = 0;
  private int otherThemisFail = 0;
  private int hbaseFail = 0;
  private int successCount = 0;

  public WriteWorker(Configuration conf) {
    this.conf = HBaseConfiguration.create(conf);
    random = new Random(System.currentTimeMillis());
    WorkerManager.registerWorker(this);
    if (Validator.usingThreadPool) {
      this.conf.setBoolean(TransactionConstant.THEMIS_ENABLE_CONCURRENT_RPC, true);
    }
  }
  
  protected int selectColumnCount() {
    int columnCount = random.nextInt(Validator.maxCellCountOfTransaction) + 1;
    if (columnCount == 1) {
      columnCount = 2;
    }
    return columnCount;
  }
  
  // disallow duplicate Column, will cause error for simulator
  protected ColumnCoordinate[] selectColumns() {
    int columnCount = selectColumnCount();
    Set<ColumnCoordinate> selectedColumns = new HashSet<ColumnCoordinate>();
    while (selectedColumns.size() != columnCount) {
      int index = random.nextInt(Validator.columnCoordinates.size());
      ColumnCoordinate columnCoordinate = Validator.columnCoordinates.get(index);
      if (!selectedColumns.contains(columnCoordinate)) {
        selectedColumns.add(columnCoordinate);
      }
    }
    return selectedColumns.toArray(new ColumnCoordinate[0]);
  }
  
  @Override
  public void run() {
    LOG.info("Worker : " + getWokerName() + ", start do transaction");
    while (true) {
      ColumnCoordinate[] columns = selectColumns();
      boolean isSucess = true;
      try {
        doTransaction(columns);
        ++successCount;
        Statistics.getStatistics().successWriteCount.inc();
      } catch (IOException e) {
        isSucess = false;
        if (!processTransactionException(e)) {
          break;
        }
      }
      if (++runCount % 10 == 0) {
        LOG.warn("Worker : " + getWokerName() + ", finish transaction, isSuccess=" + isSucess
            + ",\nrunCount=" + runCount + ";\nsuccessCount=" + successCount + ";\nfailCount="
            + failCount + ";\nlockConflict=" + lockConflictFail + ";\nwriteConflict="
            + writeConflictFail + "\notherThemis=" + otherThemisFail + ";\nhbaseFail="
            + hbaseFail);
      }
      Statistics.getStatistics().writeTransactionCount.inc();
    }
    closeWrokerAndCreateNext();
  }
  
  protected boolean processTransactionException(IOException e) {
    ++failCount;
    Statistics.getStatistics().failWriteCount.inc();
    if (e instanceof LockConflictException) {
      LOG.error(getWokerName() + " do transaction fail, encounter lock confict exception, will continue", e);
      ++lockConflictFail;
      Statistics.getStatistics().lockConflictCount.inc();
    } else if (e instanceof WriteConflictException) {
      LOG.error(getWokerName() + " do transaction fail, encounter write confict exception, will continue", e);
      ++writeConflictFail;
      Statistics.getStatistics().writeConflictCount.inc();
    } else if (e instanceof ThemisException) {
      LOG.error(getWokerName() + " do transaction fail, encounter other themis exception, will continue", e);
      ++otherThemisFail;
      Statistics.getStatistics().otherThemisFailCount.inc();
    } else if (e instanceof GeneratedTestException) {
      LOG.error(getWokerName() + " encounter generated exception, will exit", e);
      return false;
    } else {
      LOG.error(getWokerName() + " do transaction fail, encounter hbase exception", e);
      ++hbaseFail;
      Statistics.getStatistics().hbaseFailCount.inc();
    }
    return true;
  }
  
  protected void closeWrokerAndCreateNext() {
    Statistics.getStatistics().wrokerExitCount.inc();
    // unregister worker
    WorkerManager.unregisterWorker(this);
    // register a new worker
    WriteWorker writer = new WriteWorker(conf);
    LOG.warn(getWokerName() + " will exit, and will create a new worker:" + writer.getWokerName()
        + ", runTransactionCount=" + runCount);
    writer.start();
  }
  
  public String getWokerName() {
    return  "WriteWorker-" + getId();
  }

  protected int[] getColumnValues(TransactionWithFail transaction, ColumnCoordinate[] columns) throws IOException {
    int[] values = new int[columns.length];
    for (int i = 0; i < columns.length; ++i) {
      values[i] = ReadOnlyTransaction.readColumnValue(transaction, columns[i]);
    }
    return values;
  }
  
  // reset values by shuffle
  protected Integer[] shuffleValues(int[] values) {
    List<Integer> newValues = new ArrayList<Integer>();
    for (int i = 0; i < values.length; ++i) {
      newValues.add(values[i]);
    }
    Collections.shuffle(newValues);
    return newValues.toArray(new Integer[0]);
  }

  protected void deleteOrBalanceValue(Integer[] values) {
    int zeroValueIndex = -1;
    int maxValue = -1;
    int maxValueIndex = -1;
    for (int i = 0; i < values.length; ++i) {
      if (values[i] > maxValue) {
        maxValue = values[i];
        maxValueIndex = i;
      }
      if (values[i] == 0) {
        zeroValueIndex = i;
      }
    }
    if (zeroValueIndex >= 0) {
      if (maxValue > 0) {
        // balance max value to zero index
        int removeValue = maxValue / 2;
        values[maxValueIndex] -= removeValue;
        values[zeroValueIndex] = removeValue;
      }
    } else {
      int temp = values[0];
      values[0] = 0; // indicate to delete this column
      values[1] += temp;
    }
  }
  
  protected Integer[] readAndShuffleColumnsValues(TransactionWithFail transaction, ColumnCoordinate[] columns)
      throws IOException {
    LOG.info(getWokerName() + ", transaction startTs=" + transaction.getStartTs());
    long startTs = System.currentTimeMillis();
    int[] oldValues = getColumnValues(transaction, columns);
    Statistics.getStatistics().transactionReadLatency.update(System.currentTimeMillis() - startTs);
    Integer[] newValues = shuffleValues(oldValues);
    deleteOrBalanceValue(newValues);
    return newValues;
  }
  
  protected void writeNewColumnValues(TransactionWithFail transaction, ColumnCoordinate[] columns,
      Integer[] newValues) throws IOException {
    long startTs = System.currentTimeMillis();
    for (int i = 0; i < columns.length; ++i) {
      ColumnCoordinate columnCoordinate = columns[i];
      writeColumnToLocal(transaction, columnCoordinate, newValues[i].intValue());
    }
    transaction.commit();
    LOG.info(getWokerName() + ", transaction commitTs=" + transaction.getCommitTs());
    Statistics.getStatistics().transactionWriteLatency.update(System.currentTimeMillis() - startTs);
  }
  
  public void doTransaction(ColumnCoordinate[] columns) throws IOException {
    TransactionWithFail transaction = new TransactionWithFail(conf, Validator.connection);
    Integer[] newValues = readAndShuffleColumnsValues(transaction, columns);
    writeNewColumnValues(transaction, columns, newValues);
    ValueChecker.checkUpdateValues(getWokerName(), columns, newValues, transaction.getCommitTs());
  }
  
  protected static void writeColumnToLocal(TransactionWithFail transaction, ColumnCoordinate columnCoordinate, int value)
      throws IOException {
    if (value == 0) {
      ThemisDelete delete = new ThemisDelete(columnCoordinate.getRow()).deleteColumn(
        columnCoordinate.getFamily(), columnCoordinate.getQualifier());
      transaction.delete(columnCoordinate.getTableName(), delete);
      Statistics.getStatistics().deleteColumnCount.inc();
    } else {
      ThemisPut put = new ThemisPut(columnCoordinate.getRow()).add(columnCoordinate.getFamily(),
        columnCoordinate.getQualifier(), Bytes.toBytes(value));
      transaction.put(columnCoordinate.getTableName(), put);
    }
  }
}