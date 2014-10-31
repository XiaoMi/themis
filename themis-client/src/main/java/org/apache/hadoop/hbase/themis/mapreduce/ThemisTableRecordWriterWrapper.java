package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.exception.LockCleanedException;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ThemisTableRecordWriterWrapper<K, V> extends RecordWriter<K, V> {
  private static final Log LOG = LogFactory.getLog(ThemisTableRecordWriterWrapper.class);
  public static final String LOG_PER_WRITE_TRANSACTION_COUNT = "themis.mapreduce.log.write.rowcount";
  public static final String THEMIS_WRITE_RETRY_COUNT = "themis.write.retry.count";
  public static final String THEMIS_WRITE_RETRY_PAUSE = "themis.write.retry.pause";
  private final RecordWriter<K, V> impl;
  private final int logPerWriteTransaction;
  private final int writeRetryCount;
  private final int writeRetryPause;
  private long timestamp = 0;
  private int transactionCount = 0;
  private long totalTransactionCount = 0;
  
  public ThemisTableRecordWriterWrapper(RecordWriter<K, V> impl, Configuration conf) {
    this.impl = impl;
    this.logPerWriteTransaction = conf.getInt(LOG_PER_WRITE_TRANSACTION_COUNT, 100);
    this.writeRetryCount = conf.getInt(THEMIS_WRITE_RETRY_COUNT, 1);
    this.writeRetryPause = conf.getInt(THEMIS_WRITE_RETRY_PAUSE, 1000);
    timestamp = System.currentTimeMillis();
  }
  
  @Override
  public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
    this.impl.close(arg0);
  }

  @Override
  public void write(K arg0, V arg1) throws IOException, InterruptedException {
    try {
      int retry = 0;
      while (true) {
        try {
          this.impl.write(arg0, arg1);
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
      ++totalTransactionCount;
      if (++transactionCount >= this.logPerWriteTransaction) {
        long now = System.currentTimeMillis();
        LOG.info("Themis Reduce took " + (now - timestamp) + "ms to process " + transactionCount
            + " transactions, totalTransaction=" + totalTransactionCount);
        transactionCount = 0;
        timestamp = now;
      }
    }
  }
}