package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ThemisTableRecordWriterWrapper<K, V> extends RecordWriter<K, V> {
  private static final Log LOG = LogFactory.getLog(ThemisTableRecordWriterWrapper.class);
  public static final String LOG_PER_WRITE_TRANSACTION_COUNT = "themis.mapreduce.log.write.rowcount";
  private final RecordWriter<K, V> impl;
  private final int logPerWriteTransaction;
  private long timestamp = 0;
  private int transactionCount = 0;
  
  public ThemisTableRecordWriterWrapper(RecordWriter<K, V> impl, Configuration conf) {
    this.impl = impl;
    this.logPerWriteTransaction = conf.getInt(LOG_PER_WRITE_TRANSACTION_COUNT, 100);
    timestamp = System.currentTimeMillis();
  }
  
  @Override
  public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
    this.impl.close(arg0);
  }

  @Override
  public void write(K arg0, V arg1) throws IOException, InterruptedException {
    try {
      this.impl.write(arg0, arg1);
    } finally {
      if (++transactionCount >= this.logPerWriteTransaction) {
        long now = System.currentTimeMillis();
        LOG.info("Themis Reduce took " + (now - timestamp) + "ms to process " + transactionCount
            + " transactions");
        transactionCount = 0;
        timestamp = now;
      }
    }
  }
}