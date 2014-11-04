package org.apache.hadoop.hbase.themis.timestamp;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.ThemisStatistics;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

// batch request timestamp to achieve better performance. timestamp requests of users will be
// buffered in a queue; then user will wait to get a timestamp and an underlining work thread
// will make batch timestamp request and allocate timestamps to waiting users.
public abstract class BaseTimestampOracle {
  private static final Log LOG = LogFactory.getLog(BaseTimestampOracle.class);
  private final ThreadPoolExecutor executor;
  private final ArrayBlockingQueue<Runnable> requestQueue;
  private final int maxQueuedRequestCount;
  private final int requestTimeout;
  private static long currentRequestId = 0;
  private static long currentTimestamp = 0;
  private static int cachedTimestampCount = 0;
  
  public BaseTimestampOracle(Configuration conf) throws IOException {
    maxQueuedRequestCount = conf.getInt(TransactionConstant.MAX_TIMESTAMP_REQUEST_QUEUE_KEY,
      TransactionConstant.DEFAULT_MAX_TIMESTAMP_REQUEST_QUEUE_LEN);
    requestTimeout = conf.getInt(TransactionConstant.TIMESTAMP_REQUEST_TIMEOUT,
      TransactionConstant.DEFAULT_TIMESTAMP_REQUEST_TIMEOUT);
    requestQueue = new ArrayBlockingQueue<Runnable>(maxQueuedRequestCount);
    // must need only one work thread
    this.executor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS, requestQueue,
      Threads.newDaemonThreadFactory("themis-timestamp-request-worker"),
      new RequestRejectedHandler());
  }
  
  public Pair<Long, Long> getRequestIdWithTimestamp() throws IOException {
    return getRequestIdWithTimestamp(0);
  }
  
  public void close() throws IOException {
    if (this.executor != null) {
      this.executor.shutdownNow();
    }
  }
  
  protected Pair<Long, Long> getRequestIdWithTimestamp(long timeout) throws IOException {
    Future<Pair<Long, Long>> future = null;
    try {
      // submit request to queue
      future = this.executor.submit(new TimestampRequest());
      // wait a period of requestTimeout to get timestamp
      return future.get(timeout != 0 ? timeout : requestTimeout, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      LOG.fatal("get timestamp fail", t);
      throw new IOException(t);
    }
  }
  
  public abstract long getTimestamps(int n) throws IOException;

  class TimestampRequest implements Callable<Pair<Long, Long>> {
    public Pair<Long, Long> call() throws Exception {
      // only one worker to response to the request queue, so that do not need synchronized. should
      // issue a batch timestamp request for the current queue if no cached timestamps available.
      // TODO(cuijianwei): could issue a batch timestamp by another thread before all cached timestamps consumed
      if (cachedTimestampCount == 0) {
        int batchRequestSize = requestQueue.size() + 1;
        currentTimestamp = getTimestamps(batchRequestSize);
        currentRequestId = currentTimestamp;
        cachedTimestampCount = batchRequestSize;
        ThemisStatistics.getStatistics().batchSizeOfTimestampRequest.inc(batchRequestSize);
      }
      --cachedTimestampCount;
      return new Pair<Long, Long>(currentRequestId, currentTimestamp++);
    }
    
  }
  
  class RequestRejectedHandler implements RejectedExecutionHandler {
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      throw new RejectedExecutionException(
          "get timestamp request is rejected, queued request count : " + requestQueue.size());
    }
  }
  
  // a local implementation of timestamp oracle
  public static class LocalTimestampOracle extends BaseTimestampOracle {
    private long currentTs = 0;
    private Object lock = new Object();

    public LocalTimestampOracle(Configuration conf) throws IOException {
      super(conf);
    }
    
    public long getTimestamps(int n) throws IOException {
      long timestamp = System.currentTimeMillis() << 18;
      long result = 0;
      synchronized (lock) {
        if (timestamp > currentTs) {
          currentTs = timestamp;
        }
        result = currentTs;
        currentTs += n;
      }
      return result;
    }
  }
  
  // this class make sure the startTs and commitTs is retrieved from two different batch.
  // this is very important for the correctness of themis read. Although the current
  // implementation of BaseTimestampOracle will not return startTs and commitTs from the
  // same batch to the same Transaction; we also need this wrapper class to insure this
  public static class ThemisTimestamp {
    private BaseTimestampOracle impl;
    private long lastRequestId = 0;
    
    public ThemisTimestamp(BaseTimestampOracle impl) {
      this.impl = impl;
    }
    
    public long getStartTs() throws IOException {
      Pair<Long, Long> result = impl.getRequestIdWithTimestamp();
      lastRequestId = result.getFirst();
      return result.getSecond();
    }
    
    public long getCommitTs() throws IOException {
      Pair<Long, Long> result = impl.getRequestIdWithTimestamp();
      // make sure startTs and commitTs come from different batch request
      if (result.getFirst() == lastRequestId) {
        throw new ThemisFatalException("get commitTs in the same batch request with " +
        		"startTs, startTsRequestId=" + lastRequestId + ", commitTs=" + result.getSecond());
      }
      return result.getSecond();
    }
  }
}