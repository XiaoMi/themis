package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.themis.ThemisScan;
import org.apache.hadoop.hbase.themis.ThemisScanner;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TableRecordReaderImpl is not easy to inherit, so that we copy its code and make change when necessary.
// TODO : create a jira to make TableRecordReaderImpl inheritable?
public class ThemisTableRecordReaderImpl {
  public static final String LOG_PER_ROW_COUNT = "hbase.mapreduce.log.scanner.rowcount";

  private static final Logger LOG = LoggerFactory.getLogger(ThemisTableRecordReaderImpl.class);

  // HBASE_COUNTER_GROUP_NAME is the name of mapreduce counter group for HBase
  private static final String HBASE_COUNTER_GROUP_NAME = "Themis Counters";
  private Connection connection = null;
  private Transaction transaction = null;
  private ThemisScanner scanner = null;
  private TableName tableName;
  private Configuration conf;
  private Scan scan = null;
  private Scan currentScan = null;
  private byte[] lastSuccessfulRow = null;
  private ImmutableBytesWritable key = null;
  private Result value = null;
  private TaskAttemptContext context = null;
  private Method getCounter = null;
  private long numRestarts = 0;
  private long timestamp;
  private int rowcount;
  private long totalRowCount;
  private boolean logScannerActivity = false;
  private int logPerRowCount = 100;

  public void restart(byte[] firstRow) throws IOException {
    if (connection == null) {
      connection = ConnectionFactory.createConnection(conf);
    }

    currentScan = new Scan(scan);
    currentScan.withStartRow(firstRow);
    currentScan.setScanMetricsEnabled(true);
    if (this.scanner != null) {
      if (logScannerActivity) {
        LOG.info("Closing the previously opened scanner object.");
      }
      this.scanner.close();
    }

    // TODO : should use the same timestamp when the Transaction first constructed?
    this.transaction = new Transaction(connection);
    this.scanner = transaction.getScanner(tableName, new ThemisScan(currentScan));
    if (logScannerActivity) {
      LOG.info("Current scan=" + currentScan.toString());
      timestamp = System.currentTimeMillis();
      rowcount = 0;
      totalRowCount = 0;
    }
  }

  public void setTableName(TableName tableName) {
    this.tableName = tableName;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    logScannerActivity = conf.getBoolean(ScannerCallable.LOG_SCANNER_ACTIVITY, false);
    logPerRowCount = conf.getInt(LOG_PER_ROW_COUNT, 100);
  }

  // the following methods are all coped from TableRecordReaderImpl.java
  private Method retrieveGetCounterWithStringsParams(TaskAttemptContext context)
      throws IOException {
    Method m = null;
    try {
      m = context.getClass().getMethod("getCounter", new Class[] { String.class, String.class });
    } catch (SecurityException e) {
      throw new IOException("Failed test for getCounter", e);
    } catch (NoSuchMethodException e) {
      // Ignore
    }
    return m;
  }

  public void setScan(Scan scan) {
    this.scan = scan;
  }

  public void initialize(InputSplit inputsplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (context != null) {
      this.context = context;
      getCounter = retrieveGetCounterWithStringsParams(context);
    }
    restart(scan.getStartRow());
  }

  public void close() {
    if (this.scanner != null) {
      this.scanner.close();
    }
    try {
      if (this.connection != null) {
        this.connection.close();
      }
    } catch (IOException e) {
      LOG.error("close connection error", e);
    }
  }

  public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  public Result getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (key == null) key = new ImmutableBytesWritable();
    if (value == null) value = new Result();
    try {
      try {
        value = this.scanner.next();
        if (logScannerActivity) {
          rowcount++;
          totalRowCount++;
          if (rowcount >= logPerRowCount) {
            long now = System.currentTimeMillis();
            LOG.info("Mapper took " + (now - timestamp) + "ms to process " + rowcount + " rows" +
              ", totalReadRows=" + totalRowCount);
            timestamp = now;
            rowcount = 0;
          }
        }
      } catch (IOException e) {
        // try to handle all IOExceptions by restarting
        // the scanner, if the second call fails, it will be rethrown
        LOG.info("recovered from " + StringUtils.stringifyException(e));
        if (lastSuccessfulRow == null) {
          LOG.warn("We are restarting the first next() invocation," +
            " if your mapper has restarted a few other times like this" +
            " then you should consider killing this job and investigate" +
            " why it's taking so long.");
        }
        if (lastSuccessfulRow == null) {
          restart(scan.getStartRow());
        } else {
          restart(lastSuccessfulRow);
          scanner.next(); // skip presumed already mapped row
        }
        value = scanner.next();
        numRestarts++;
      }
      if (value != null && value.size() > 0) {
        key.set(value.getRow());
        lastSuccessfulRow = key.get();
        return true;
      }

      updateCounters();
      return false;
    } catch (IOException ioe) {
      if (logScannerActivity) {
        long now = System.currentTimeMillis();
        LOG.info("Mapper took " + (now - timestamp) + "ms to process " + rowcount +
          " rows, totalReadRows=" + totalRowCount);
        LOG.info("", ioe);
        String lastRow =
          lastSuccessfulRow == null ? "null" : Bytes.toStringBinary(lastSuccessfulRow);
        LOG.info("lastSuccessfulRow=" + lastRow);
      }
      throw ioe;
    }
  }

  private void updateCounters() throws IOException {
    // we can get access to counters only if hbase uses new mapreduce APIs
    if (this.getCounter == null) {
      return;
    }

    ScanMetrics scanMetrics = scanner.getScanMetrics();
    try {
      for (Map.Entry<String, Long> entry : scanMetrics.getMetricsMap().entrySet()) {
        Counter ct = (Counter) getCounter.invoke(context, HBASE_COUNTER_GROUP_NAME, entry.getKey());

        ct.increment(entry.getValue());
      }
      ((Counter) getCounter.invoke(context, HBASE_COUNTER_GROUP_NAME, "NUM_SCANNER_RESTARTS"))
        .increment(numRestarts);
    } catch (Exception e) {
      LOG.debug("can't update counter." + StringUtils.stringifyException(e));
    }
  }

  public float getProgress() {
    // Depends on the total number of tuples
    return 0;
  }
}
