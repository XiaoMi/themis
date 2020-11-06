package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class ThemisTableRecordWriterBase<K, V> extends RecordWriter<K, V> {
  private static final Log LOG = LogFactory.getLog(ThemisTableRecordWriterBase.class);
  public static final String LOG_PER_WRITE_TRANSACTION_COUNT = "themis.mapreduce.log.write.rowcount";
  public static final String THEMIS_WRITE_RETRY_COUNT = "themis.write.retry.count";
  public static final String THEMIS_WRITE_RETRY_PAUSE = "themis.write.retry.pause";
  protected Connection connection;
  private final int logPerWriteTransaction;
  private final int writeRetryCount;
  private final int writeRetryPause;
  private final WriteStatistics statistics = new WriteStatistics();
  
  public static class SingleColumnWriteStat {
    public int currentCount;
    public long currentTotalLatency;
    public long total;
    
    public void update(int latency) {
      ++currentCount;
      currentTotalLatency += latency;
      ++total;
    }
    
    public void resetCurrent() {
      this.currentCount = 0;
      this.currentTotalLatency = 0;
    }
    
    @Override
    public String toString() {
      int avgLatency = (currentCount == 0 ? 0 : (int)(currentTotalLatency / currentCount));
      return "totalCount=" + total + ", currentCount=" + currentCount + ", currentAvgLatency(ms)="
          + avgLatency;
    }
  }
  
  public static class SingleRowWriteStat extends SingleColumnWriteStat {
    public int currentTotalColumn;
    
    public void update(int latency, int columnCount) {
      super.update(latency);
      currentTotalColumn += columnCount;
    }

    @Override
    public void resetCurrent() {
      super.resetCurrent();
      this.currentTotalColumn = 0;
    }
    
    @Override
    public String toString() {
      int avgColumn = (currentCount == 0 ? 0 : currentTotalColumn / currentCount);
      return super.toString() + ", curerntAvgColumnCount=" + avgColumn;
    }
  }
  
  public static class MultiRowWriteStat extends SingleRowWriteStat {
    public int currentTotalRow;
    
    public void update(int latency, int rowCount, int columnCount) {
      super.update(latency, columnCount);
      currentTotalRow += rowCount;
    }

    @Override
    public void resetCurrent() {
      super.resetCurrent();
      currentTotalRow = 0;
    }
    
    @Override
    public String toString() {
      int avgRow = (currentCount == 0 ? 0 : currentTotalRow / currentCount);
      return super.toString() + ", currentAvgRowCount=" + avgRow;
    }
  }
  
  public static class WriteStatistics {
    public SingleColumnWriteStat singleColumn = new SingleColumnWriteStat();
    public SingleRowWriteStat singleRow = new SingleRowWriteStat();
    public MultiRowWriteStat multiRow = new MultiRowWriteStat();
    public long totalTransactionCount = 0;
    public int currentTransactionCount = 0;
    public long lastMs = System.currentTimeMillis();
    
    public void update(Transaction transaction, int latency) {
      ++totalTransactionCount;
      ++currentTransactionCount;
      Pair<Integer, Integer> mutationsCount = transaction.getMutations().getMutationsCount();
      if (mutationsCount.getFirst() == 1) {
        if (mutationsCount.getSecond() == 1) {
          singleColumn.update(latency);
        } else {
          singleRow.update(latency, mutationsCount.getSecond());
        }
      } else {
        multiRow.update(latency, mutationsCount.getFirst(), mutationsCount.getSecond());
      }
    }
    
    public void resetCurrent() {
      currentTransactionCount = 0;
      lastMs = System.currentTimeMillis();
      singleColumn.resetCurrent();
      singleRow.resetCurrent();
      multiRow.resetCurrent();
    }
    
    public void logStatistics() {
      LOG.info("Themis Reduce took " + (System.currentTimeMillis() - lastMs) + "ms to process "
          + currentTransactionCount + " transactions, totalTransaction=" + totalTransactionCount + ", stats:");
      LOG.info("SingleColumnTransaction : " + singleColumn.toString());
      LOG.info("SingleRowTransaction : " + singleRow.toString());
      LOG.info("MultiRowTransaction : " + multiRow.toString());
    }
  }
  
  public ThemisTableRecordWriterBase(Configuration conf) throws IOException {
    connection = ConnectionFactory.createConnection(conf);
    this.logPerWriteTransaction = conf.getInt(LOG_PER_WRITE_TRANSACTION_COUNT, 100);
    this.writeRetryCount = conf.getInt(THEMIS_WRITE_RETRY_COUNT, 1);
    this.writeRetryPause = conf.getInt(THEMIS_WRITE_RETRY_PAUSE, 1000);
  }
  
  @Override
  public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
    if (connection != null) {
      connection.close();
    }
  }

  @Override
  public void write(K k, V v) throws IOException, InterruptedException {
    long beginMs = System.currentTimeMillis();
    Transaction transaction = null;
    try {
      int retry = 0;
      while (true) {
        try {
          transaction = new Transaction(connection);
          doWrite(k, v, transaction);
          transaction.commit();
          break;
        } catch (LockCleanedException e) {
          LOG.error("prewrite lock has been cleaned", e);
          if (++retry > this.writeRetryCount) {
            throw e;
          }
          LOG.info("sleep " + this.writeRetryPause + " to retry transaction");
          Threads.sleep(this.writeRetryPause);
        }
      }
    } finally {
      if (transaction != null) {
        statistics.update(transaction, (int)(System.currentTimeMillis() - beginMs));
      }
      // will retry once default
      if (statistics.currentTransactionCount >= this.logPerWriteTransaction) {
        statistics.logStatistics();
        statistics.resetCurrent();
      }
    }
  }
  
  public abstract void doWrite(K k, V v, Transaction transaction) throws IOException,
      InterruptedException;
}