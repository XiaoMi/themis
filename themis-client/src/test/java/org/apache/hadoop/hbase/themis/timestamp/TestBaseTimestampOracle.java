package org.apache.hadoop.hbase.themis.timestamp;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle.LocalTimestampOracle;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle.ThemisTimestamp;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Before;
import org.junit.Test;

public class TestBaseTimestampOracle {
  private BaseTimestampOracle timestampOracle;
  private GetTimestampInvokeThread[] getThreads;
  
  @Before
  public void setUp() throws IOException {
    timestampOracle = new LocalTimestampOracle(HBaseConfiguration.create());
  }
  
  static class GetTimestampInvokeThread extends Thread {
    private final BaseTimestampOracle impl;
    private final int invokeCount;
    public final Object[] results;
    private final int timeout;
    
    public GetTimestampInvokeThread(BaseTimestampOracle impl, int invokeCount) {
      this(impl, invokeCount, 0);
    }
    
    public GetTimestampInvokeThread(BaseTimestampOracle impl, int invokeCount, int timeout) {
      this.impl = impl;
      this.invokeCount = invokeCount;
      this.results = new Object[invokeCount];
      this.timeout = timeout;
    }
    
    @Override
    public void run() {
      for (int i = 0; i < invokeCount; ++i) {
        Object result = null;
        try {
          result = this.impl.getRequestIdWithTimestamp(timeout);
          if (timeout == 0) {
            this.results[i] = ((Pair<Long, Long>)result).getSecond();
          } else {
            this.results[i] = result;
          }
        } catch (Throwable t) {
          this.results[i] = t;
        } 
      }
    }
  }
  
  protected void createGetThreads(int threadCount, int invokeCount) {
    getThreads = new GetTimestampInvokeThread[threadCount];
    for (int i = 0; i < getThreads.length; ++i) {
      getThreads[i] = new GetTimestampInvokeThread(timestampOracle, invokeCount);
    }
  }
  
  protected void startGetThreads(boolean keepOrder) {
    for (GetTimestampInvokeThread getThread : getThreads) {
      getThread.start();
      if (keepOrder) {
        // wait async request submit to keep the invoke order
        Threads.sleep(5);
      }
    }
  }
  
  protected void startGetThreads() {
    startGetThreads(false);
  }
  
  protected void waitGetThreads() throws Exception {
    for (GetTimestampInvokeThread getThread : getThreads) {
      getThread.join();
    }
  }
  
  @Test
  public void testGetTimestampWithMultiThread() throws Exception {
    int[] threadCounts = new int[]{1, 10};
    for (int i = 0; i < threadCounts.length; ++i) {
      int threadCount = threadCounts[i];
      int getCountPerThread = 1000;
      createGetThreads(threadCount, getCountPerThread);
      startGetThreads();
      waitGetThreads();
      int expectDistinctTsCount = threadCount * getCountPerThread;
      Set<Long> tsSet = new HashSet<Long>(expectDistinctTsCount);
      for (GetTimestampInvokeThread getThread : getThreads) {
        Object[] results = getThread.results;
        long lastTs = 0;
        for (int j = 0; j < results.length; ++j) {
          Assert.assertTrue(results[j] instanceof Long);
          long ts = (Long)(results[j]);
          Assert.assertTrue(ts > lastTs);
          lastTs = ts;
          tsSet.add(ts);
        }
      }
      Assert.assertEquals(expectDistinctTsCount , tsSet.size());
    }
  }
  
  private static final String TIMESTAMP_EXCEPTION_STRING = "Timestamp Exception";
  
  protected void createBatchOracleWithTimeoutImpl(int queueSize, int requestTimeout, int implTimeout) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(TransactionConstant.TIMESTAMP_REQUEST_TIMEOUT, requestTimeout);
    conf.setInt(TransactionConstant.MAX_TIMESTAMP_REQUEST_QUEUE_KEY, queueSize);
    timestampOracle = new TimeoutImpl(conf, implTimeout);
  }
  
  static class TimeoutImpl extends BaseTimestampOracle {
    private long lastValue = 0;
    private int timeout;
    
    public TimeoutImpl(Configuration conf, int timeout) throws IOException {
      super(conf);
      this.timeout = timeout;
    }
    
    public long getTimestamps(int n) throws IOException {
      Threads.sleep(timeout);
      long temp = lastValue;
      lastValue += n;
      return temp;
    }
  }
  
  static class ExceptionImpl extends BaseTimestampOracle {
    public ExceptionImpl(Configuration conf) throws IOException {
      super(conf);
    }
    
    public long getTimestamps(int n) throws IOException {
      throw new IOException(TIMESTAMP_EXCEPTION_STRING);
    }
  }

  public Throwable getRootCause(Object result) {
    Assert.assertTrue(result instanceof Throwable);
    Throwable th = (Throwable)result;
    while (th.getCause() != null) {
      th = th.getCause();
    }
    return th;
  }
  
  @Test
  public void testGetTimestampWithBatchImplTimeout() throws Exception {
    // test single thread
    createBatchOracleWithTimeoutImpl(1, 100, 150);
    long startTs = System.currentTimeMillis();
    try {
      timestampOracle.getRequestIdWithTimestamp();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof TimeoutException);
    } finally {
      long consume = System.currentTimeMillis() - startTs;
      Assert.assertTrue(consume >= 100 && consume < 150);
    }
    
    // test multi thread
    createBatchOracleWithTimeoutImpl(1, 100, 150);
    createGetThreads(3, 1);
    startGetThreads(true);
    waitGetThreads();
    Assert.assertTrue(getRootCause(getThreads[0].results[0]) instanceof TimeoutException);
    Assert.assertTrue(getRootCause(getThreads[1].results[0]) instanceof TimeoutException);
    Assert.assertTrue(getRootCause(getThreads[2].results[0]) instanceof RejectedExecutionException);
  }
  
  @Test
  public void testTwoRequestsOfTheSameThreadMustGetTimestampFromTheDifferentBatchRequest() throws Exception {
    createBatchOracleWithTimeoutImpl(2, 100, 150);
    GetTimestampInvokeThread threadA = new GetTimestampInvokeThread(timestampOracle, 1, 500);
    GetTimestampInvokeThread threadB = new GetTimestampInvokeThread(timestampOracle, 1, 50);
    threadA.start();
    threadB.start();
    threadA.join();
    threadB.join();
    long firstRequestId = ((Pair<Long, Long>) threadA.results[0]).getFirst();
    Assert.assertTrue(getRootCause(threadB.results[0]) instanceof TimeoutException);
    threadA.run();
    long secondRequestId = ((Pair<Long, Long>) threadA.results[0]).getFirst();
    Assert.assertTrue(firstRequestId != secondRequestId);
  }
  
  @Test
  public void testGetTimestampWithBatchImplException() throws Exception {
    // test multi thread
    timestampOracle = new ExceptionImpl(HBaseConfiguration.create());
    createGetThreads(10, 10);
    startGetThreads();
    waitGetThreads();
    for (GetTimestampInvokeThread thread : getThreads) {
      for (Object result : thread.results) {
        Throwable rootException = getRootCause(result);
        Assert.assertTrue(rootException instanceof IOException);
        Assert.assertEquals(TIMESTAMP_EXCEPTION_STRING, rootException.getMessage());
      }
    }
  }

  static class SameTimstampImpl extends BaseTimestampOracle {
    public SameTimstampImpl(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public long getTimestamps(int n) throws IOException {
      return 100;
    }
  }
  
  @Test
  public void testThemisTimestamp() throws Exception {
    timestampOracle = new SameTimstampImpl(HBaseConfiguration.create());
    ThemisTimestamp pTimestamp = new ThemisTimestamp(timestampOracle);
    Assert.assertEquals(100, pTimestamp.getStartTs());
    try {
      pTimestamp.getCommitTs();
      Assert.fail();
    } catch (ThemisFatalException e) {}
  }  
}
