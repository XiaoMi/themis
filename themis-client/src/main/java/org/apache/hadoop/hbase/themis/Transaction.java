package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.ConcurrentRowCallables.TableAndRow;
import org.apache.hadoop.hbase.themis.cache.ColumnMutationCache;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.cp.ThemisCpUtil;
import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.exception.MultiRowExceptions;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.themis.lock.PrimaryLock;
import org.apache.hadoop.hbase.themis.lock.SecondaryLock;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.themis.lockcleaner.LockCleaner;
import org.apache.hadoop.hbase.themis.lockcleaner.WallClock;
import org.apache.hadoop.hbase.themis.lockcleaner.WorkerRegister;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle.ThemisTimestamp;
import org.apache.hadoop.hbase.themis.timestamp.TimestampOracleFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

public class Transaction extends Configured implements TransactionInterface {
  private static final Log LOG = LogFactory.getLog(Transaction.class);
  private HConnection connection;
  private ThemisTimestamp timestampOracle;
  protected LockCleaner lockCleaner;
  // wallClock will be include into lock to judge whether the transaction is expired
  private WallClock wallClock;
  protected long wallTime;
  private WorkerRegister register;
  protected WrappedCoprocessorClient cpClient;
  protected long startTs;
  protected long commitTs;
  protected ColumnMutationCache mutationCache;
  // the selected primary column coordinate. subclasses and unit tests could set the primary manually
  protected ColumnCoordinate primary;
  private int primaryIndexInRow = -1; // the index of selected primary column in primary row
  protected List<ColumnCoordinate> secondaries; // coordinates of secondary columns
  protected RowMutation primaryRow; // the row contains the primary column
  protected List<Pair<byte[], RowMutation>> secondaryRows;
  protected byte[] secondaryLockBytesWithoutType;
  protected volatile static ExecutorService singletonThreadPool;
  private static Object singletonThreadPoolLock = new Object();
  protected boolean enableConcurrentRpc;
  protected boolean singleRowTransaction = false;
  protected boolean enableSingleRowWrite;
  
  // will use the connection.getConfiguation() to config the Transaction
  public Transaction(HConnection connection) throws IOException {
    this(connection.getConfiguration(), connection);
  }
  
  public Transaction(Configuration conf, HConnection connection)
      throws IOException {
    this(conf, connection, TimestampOracleFactory.getTimestampOracle(conf), WallClock
      .getWallTimer(conf), WorkerRegister.getWorkerRegister(conf));
  }
  
  protected Transaction(Configuration conf, HConnection connection,
      BaseTimestampOracle timestampOracle, WallClock wallClock, WorkerRegister register) throws IOException {
    setConf(conf);
    this.connection = connection;
    this.timestampOracle = new ThemisTimestamp(timestampOracle);
    this.wallClock = wallClock;
    this.register = register;
    this.register.registerWorker();
    this.enableConcurrentRpc = getConf().getBoolean(TransactionConstant.THEMIS_ENABLE_CONCURRENT_RPC, false);
    this.cpClient = new WrappedCoprocessorClient(connection);
    this.lockCleaner = new LockCleaner(getConf(), connection, this.wallClock, this.register, this.cpClient);
    this.mutationCache = new ColumnMutationCache();
    this.startTs = this.timestampOracle.getStartTs();
    this.enableSingleRowWrite = getConf().getBoolean(TransactionConstant.ENABLE_SINGLE_ROW_WRITE, false);
  }
  
  protected static void setThreadPool(ExecutorService threadPool) {
    Transaction.singletonThreadPool = threadPool;
  }
  
  protected static ExecutorService getThreadPool() {
    if (singletonThreadPool == null) {
      synchronized (singletonThreadPoolLock) {
        if (singletonThreadPool == null) {
          singletonThreadPool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
              Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
              Threads.newDaemonThreadFactory("themis-client-transaction-shared-executor"));
        }
      }
    }
    return singletonThreadPool;
  }
  
  public void put(byte[] tableName, ThemisPut put) throws IOException {
    ThemisRequest.checkContainColumn(put);
    mergeMutation(tableName, put.getHBasePut());
  }

  public void delete(byte[] tableName, ThemisDelete delete) throws IOException {
    ThemisRequest.checkContainColumn(delete);
    mergeMutation(tableName, delete.getHBaseDelete());
  }

  // merge mutation with same column coordinate
  protected void mergeMutation(byte[] tableName, Mutation mutation) throws IOException {
    for (Entry<byte[], List<KeyValue>> entry : mutation.getFamilyMap().entrySet()) {
      for (KeyValue keyValue : entry.getValue()) {
        mutationCache.addMutation(tableName, keyValue);
      }
    }
  }
  
  public Result get(byte[] tableName, ThemisGet userGet) throws IOException {
    ThemisRequest.checkContainColumn(userGet);
    Result pResult = this.cpClient.themisGet(tableName, userGet.getHBaseGet(), startTs);
    // if the result contains KeyValues from lock columns, it means we encounter conflict
    // locks and need to clean lock before retry
    if (ThemisCpUtil.isLockResult(pResult)) {
      return tryToCleanLockAndGetAgain(tableName, userGet.getHBaseGet(), pResult.list());
    }
    return pResult;
  }

  protected Result tryToCleanLockAndGetAgain(byte[] tableName, Get get,
      List<KeyValue> lockKvs) throws IOException {
    // we only need to try once to clean lock; although later transaction may write a lock with smaller startTs
    // than Transaction.startTs, the commitTs of the write must be larger than Transaction.startTs. Therefore
    // should not be read out by this transaction
    lockCleaner.tryToCleanLocks(tableName, lockKvs);
    // after cleaning lock successfully, we must read write columns again to get the newest write-column
    // KeyValue having the commitTs < this.startTs. This time, we must ignore any lock because the commitTs of
    // corresponding transaction must greater than this.startTs and should be read out
    Result pResult = this.cpClient.themisGet(tableName, get, startTs, true);
    if (ThemisCpUtil.isLockResult(pResult)) {
      // should not encounter lock conflict as we ignore conflict lock this time
      throw new ThemisFatalException(
          "encounter conflict lock after lock clean when read, conflict lock kv count="
              + pResult.list().size() + ", lockKvs=" + pResult.list());
    }
    return pResult;
  }
  
  public ThemisScanner getScanner(byte[] tableName, ThemisScan userScan) throws IOException {
    ThemisRequest.checkContainColumn(userScan);
    return new ThemisScanner(tableName, userScan.getHBaseScan(), this);
  }
  
  public void commit() throws IOException {
    if (mutationCache.size() == 0) {
      return;
    }
    wallTime = wallClock.getWallTime();
    selectPrimaryAndSecondaries();

    // must prewrite primary successfully before prewriting secondaries
    prewritePrimary();
    if (enableConcurrentRpc) {
      concurrentPrewriteSecondaries();
    } else {
      prewriteSecondaries();
    }
    // must get commitTs after doing prewrite successfully
    commitTs = timestampOracle.getCommitTs();
    commitPrimary();
    if (enableConcurrentRpc) {
      concurrentCommitSecondaries();
    } else {
      commitSecondaries();
    }
  }
  
  public HConnection getHConnection() {
    return this.connection;
  }
  
  // destroy inter-threads used by themis
  public static void destroy() throws IOException {
    BaseTimestampOracle timestampOracle = TimestampOracleFactory.getTimestampOracleWithoutCreate();
    if (timestampOracle != null) {
      timestampOracle.close();
    }
    if (singletonThreadPool != null) {
      singletonThreadPool.shutdownNow();
    }
  }
  
  protected void asyncPrewriteRowWithLockClean(ConcurrentRowCallables<Void> calls,
      final byte[] tableName, final RowMutation rowMutation, final boolean containPrimary)
      throws IOException {
    calls.addCallable(new RowCallable<Void>(tableName, rowMutation.getRow()) {
      @Override
      public Void call() throws Exception {
        prewriteRowWithLockClean(tableName, rowMutation, containPrimary);
        return null;
      }
    });
  }
  
  protected void asyncRollback(ConcurrentRowCallables<Void> calls, final byte[] tableName,
      final RowMutation rowMutation) throws IOException {
    calls.addCallable(new RowCallable<Void>(tableName, rowMutation.getRow()) {
      @Override
      public Void call() throws Exception {
        rollbackRow(tableName, rowMutation);
        return null;
      }
    });
  }
  
  protected void concurrentPrewriteSecondaries() throws IOException {
    ConcurrentRowCallables<Void> calls = new ConcurrentRowCallables<Void>(getThreadPool());
    for (int i = 0; i < secondaryRows.size(); ++i) {
      final Pair<byte[], RowMutation> secondaryRow = secondaryRows.get(i);
      asyncPrewriteRowWithLockClean(calls, secondaryRow.getFirst(), secondaryRow.getSecond(), false);
    }
    calls.waitForResult();
    if (calls.getExceptions().size() != 0) {
      // async rollback success rows
      ConcurrentRowCallables<Void> rollbacks = new ConcurrentRowCallables<Void>(getThreadPool());
      for (Entry<TableAndRow, Void> successRows : calls.getResults().entrySet()) {
        TableAndRow tableAndRow = successRows.getKey();
        asyncRollback(rollbacks, tableAndRow.getTableName(),
          mutationCache.getRowMutation(tableAndRow));
      }
      
      // throw multi-excpetions to show failed rows
      throw new MultiRowExceptions("concurrent prewrite fail", calls.getExceptions());
    }
  }

  protected void selectPrimaryAndSecondaries() throws IOException {
    secondaryRows = new ArrayList<Pair<byte[], RowMutation>>();
    secondaries = new ArrayList<ColumnCoordinate>();
    for (Entry<byte[], Map<byte[], RowMutation>> tableEntry : mutationCache.getMutations()) {
      byte[] table = tableEntry.getKey();
      for (Entry<byte[], RowMutation> rowEntry : tableEntry.getValue().entrySet()) {
        byte[] row = rowEntry.getValue().getRow();
        boolean findPrimaryInRow = false;
        List<ColumnMutation> columnMutations = rowEntry.getValue().mutationList();
        for (int i = 0; i < columnMutations.size(); ++i) {
          ColumnCoordinate column = new ColumnCoordinate(table, row, columnMutations.get(i));
          // set the first column as primary if primary is not set by user
          if (primaryIndexInRow == -1 && (primary == null || column.equals(primary))) {
            primary = column;
            primaryIndexInRow = i;
            primaryRow = rowEntry.getValue();
            findPrimaryInRow = true;
          } else {
            secondaries.add(column);
          }
        }
        if (!findPrimaryInRow) {
          secondaryRows.add(new Pair<byte[], RowMutation>(tableEntry.getKey(), rowEntry.getValue()));
        }
      }
    }
    if (primaryIndexInRow == -1) {
      throw new IOException("can not find primary column in mutations, primary column=" + primary);
    }
    if (secondaryRows.size() == 0) {
      singleRowTransaction = true;
    }
    SecondaryLock secondaryLock = constructSecondaryLock(Type.Put);
    secondaryLockBytesWithoutType = secondaryLock == null ? null : ThemisLock.toByte(secondaryLock);
  }
  
  public void prewritePrimary() throws IOException {
    prewriteRowWithLockClean(primary.getTableName(), primaryRow, true);
  }

  public void prewriteSecondaries() throws IOException {
    for (int i = 0; i < secondaryRows.size(); ++i) {
      Pair<byte[], RowMutation> secondaryRow = secondaryRows.get(i);
      try {
        prewriteRowWithLockClean(secondaryRow.getFirst(), secondaryRow.getSecond(), false);
      } catch (IOException e) {
        // rollback prewrited primaryRow and secondaryRows
        rollbackRow(primary.getTableName(), primaryRow);
        rollbackSecondaryRows(i);
        throw e;
      }
    }
  }

  protected void prewriteRowWithLockClean(byte[] tableName, RowMutation mutation, boolean containPrimary)
      throws IOException {
    ThemisLock lock = prewriteRow(tableName, mutation, containPrimary);
    if (lock != null) {
      // we must do lock cleaning if encountering conflict lock. We must make sure the returned conflict column
      // is the data column other than the corresponding write/lock columns; otherwise, fatal errors might be caused.
      if (!ColumnUtil.isDataColumn(lock.getColumn())) {
        throw new ThemisFatalException("prewrite returned non-data conflict column, tableName="
            + Bytes.toString(tableName) + ", RowMutation=" + mutation + ", returned column=" + lock.getColumn());
      }
      lockCleaner.tryToCleanLock(lock);
      // try one more time after clean lock successfully
      lock = prewriteRow(tableName, mutation, containPrimary);
      if (lock != null) {
        throw new LockConflictException("can't clean lock, column=" + lock.getColumn()
            + ", conflict lock=" + lock);
      }
    }
  }
  
  // prewrite a PrimaryRow or SecondaryRow indicated by containPrimary
  protected ThemisLock prewriteRow(byte[] tableName, RowMutation mutation, boolean containPrimary)
      throws IOException {
    if (containPrimary) {
      if (singleRowTransaction && enableSingleRowWrite) {
        return cpClient.prewriteSingleRow(tableName, mutation.getRow(),
          mutation.mutationListWithoutValue(), startTs, ThemisLock.toByte(constructPrimaryLock()),
          secondaryLockBytesWithoutType, primaryIndexInRow);
      } else {
        return cpClient.prewriteRow(tableName, mutation.getRow(), mutation.mutationList(), startTs,
          ThemisLock.toByte(constructPrimaryLock()), secondaryLockBytesWithoutType,
          primaryIndexInRow);
      }
    } else {
      return cpClient.prewriteSecondaryRow(tableName, mutation.getRow(), mutation.mutationList(),
        startTs, secondaryLockBytesWithoutType);
    }
  }
  
  protected PrimaryLock constructPrimaryLock() {
    PrimaryLock lock = new PrimaryLock(primaryRow.getType(primary));
    setThemisLock(lock);
    for (int i = 0; i < secondaries.size(); ++i) {
      ColumnCoordinate secondary = secondaries.get(i);
      lock.addSecondaryColumn(secondary, mutationCache.getType(secondary));
    }
    return lock;
  }
  
  protected SecondaryLock constructSecondaryLock(Type type) {
    // indicates there are no secondaries, is single-column transactions
    if (primaryRow.size() <= 1 && secondaryRows.size() == 0) {
      return null;
    }
    
    SecondaryLock lock = new SecondaryLock();
    lock.setPrimaryColumn(primary);
    setThemisLock(lock);
    return lock;    
  }
  
  protected void setThemisLock(ThemisLock lock) {
    lock.setTimestamp(startTs);
    lock.setWallTime(wallTime);
    lock.setClientAddress(register.getClientAddress());
  }
  
  protected void rollbackSecondaryRows(int secondarySuccessIndex) throws IOException {
    for (int i = secondarySuccessIndex; i >= 0; --i) {
      Pair<byte[], RowMutation> secondaryRow = secondaryRows.get(i);
      rollbackRow(secondaryRow.getFirst(), secondaryRow.getSecond());
    }
  }
  
  protected void rollbackRow(byte[] tableName, RowMutation rowMutation) throws IOException {
    lockCleaner.eraseLockAndData(tableName, rowMutation.getRow(), rowMutation.getColumns(), startTs);
    ThemisStatistics.getStatistics().rollbackCount.inc();
  }
  
  public void commitPrimary() throws IOException {
    try {
      if (singleRowTransaction && enableSingleRowWrite) {
        cpClient.commitSingleRow(primary.getTableName(), primaryRow.getRow(),
          primaryRow.mutationList(), startTs, commitTs, primaryIndexInRow);
      } else {
        cpClient.commitRow(primary.getTableName(), primaryRow.getRow(),
          primaryRow.mutationListWithoutValue(), startTs, commitTs, primaryIndexInRow);
      }
    } catch (LockCleanedException e) {
      // rollback all prewrites if commit primary fail because primary lock has been erased
      LOG.warn("primary lock has been cleaned, transaction will rollback, primary column="
          + primary + ", prewriteTs=" + startTs);
      rollbackRow(primary.getTableName(), primaryRow);
      rollbackSecondaryRows(secondaryRows.size() - 1);
      throw e;
    } catch (IOException e) {
      // It is possible the server has finish the commit event when client receiving IOException,
      // such as network fail. Therefore, we can't clean secondary lock in this situation.
      throw e;
    }
  }

  protected void concurrentCommitSecondaries() throws IOException {
    ConcurrentRowCallables<Void> calls = new ConcurrentRowCallables<Void>(getThreadPool());
    for (int i = 0; i < secondaryRows.size(); ++i) {
      final Pair<byte[], RowMutation> secondaryRow = secondaryRows.get(i);
      calls.addCallable(new RowCallable<Void>(secondaryRow.getFirst(),
          secondaryRow.getSecond().getRow()) {
        @Override
        public Void call() throws Exception {
          cpClient.commitSecondaryRow(secondaryRow.getFirst(), secondaryRow.getSecond().getRow(),
            secondaryRow.getSecond().mutationListWithoutValue(), startTs, commitTs);
          return null;
        }
      });
    }
    // TODO(cuijianwei) : do not need to wait for returning
    calls.waitForResult();
    // log commit failed rows
    for (Entry<TableAndRow, IOException> failedRow : calls.getExceptions().entrySet()) {
      TableAndRow tableAndRow = failedRow.getKey();
      LOG.warn("commit secondary fail, table=" + tableAndRow.getTableName() + ", columns="
          + mutationCache.getRowMutation(tableAndRow) + ", prewriteTs=" + startTs,
        failedRow.getValue());
    }
  }
  
  public void commitSecondaries() throws IOException {
    for (int i = 0; i < secondaryRows.size(); ++i) {
      Pair<byte[], RowMutation> secondaryRow = secondaryRows.get(i);
      try {
        cpClient.commitSecondaryRow(secondaryRow.getFirst(), secondaryRow.getSecond().getRow(),
          secondaryRow.getSecond().mutationListWithoutValue(), startTs, commitTs);
      } catch (IOException e) {
        // fail of secondary commit will not stop the commits of next seconderies
        LOG.warn("commit secondary fail, table=" + Bytes.toString(secondaryRow.getFirst())
            + ", put=" + secondaryRow.getSecond() + ", prewriteTs=" + startTs
            + ", will continue committing next column", e);
      }
    }
  }
}
