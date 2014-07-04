package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.themis.exception.ThemisFatalException;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestConcurrentRowCallables extends TestBase {
  private ExecutorService threadPool;

  @Before
  public void initEnv() throws IOException {
    ArrayBlockingQueue<Runnable> requestQueue = new ArrayBlockingQueue<Runnable>(1);
    this.threadPool = new ThreadPoolExecutor(1, 2, 10, TimeUnit.SECONDS, requestQueue);
  }
  
  class TestRowCallable extends RowCallable<byte[]> {
    public TestRowCallable(byte[] tableName, byte[] rowkey) {
      super(tableName, rowkey);
    }

    @Override
    public byte[] call() throws Exception {
      return getTableAndRow().getRowkey();
    }
  }
  
  @Test
  public void testConcurrentRowCallablesSuccess() throws IOException {
    ConcurrentRowCallables<byte[]> callables = new ConcurrentRowCallables<byte[]>(threadPool);
    callables.addCallable(new TestRowCallable(TABLENAME, ROW));
    callables.addCallable(new TestRowCallable(TABLENAME, ANOTHER_ROW));
    callables.waitForResult();
    Assert.assertEquals(2, callables.getResults().size());
    Assert.assertArrayEquals(ROW, callables.getResult(TABLENAME, ROW));
    Assert.assertArrayEquals(ANOTHER_ROW, callables.getResult(TABLENAME, ANOTHER_ROW));
    Assert.assertEquals(0, callables.getExceptions().size());
  }
  
  class ExceptionRowCallable extends RowCallable<byte[]> {
    public static final String EXCEPTION_STRING = "RowCallable Exception";
    public ExceptionRowCallable(byte[] tableName, byte[] rowkey) {
      super(tableName, rowkey);
    }
    
    @Override
    public byte[] call() throws Exception {
      throw new ThemisFatalException(EXCEPTION_STRING);
    }
  }
  
  @Test
  public void testConcurrentRowCallablesWithException() throws IOException {
    ConcurrentRowCallables<byte[]> callables = new ConcurrentRowCallables<byte[]>(threadPool);
    callables.addCallable(new TestRowCallable(TABLENAME, ROW));
    callables.addCallable(new ExceptionRowCallable(TABLENAME, ANOTHER_ROW));
    callables.waitForResult();
    Assert.assertEquals(1, callables.getResults().size());
    Assert.assertArrayEquals(ROW, callables.getResult(TABLENAME, ROW));
    Assert.assertEquals(1, callables.getExceptions().size());
    Assert.assertTrue(callables.getException(TABLENAME, ANOTHER_ROW).getCause().getCause()
      instanceof ThemisFatalException);
  }
  
  class TimeoutRowCallable extends RowCallable<byte[]> {
    public TimeoutRowCallable(byte[] tableName, byte[] rowkey) {
      super(tableName, rowkey);
    }

    @Override
    public byte[] call() throws Exception {
      Threads.sleep(200);
      return getTableAndRow().getRowkey();
    }
  }
  
  @Test
  public void testConcurrentRowCallablesWithAddFail() throws IOException {
    ConcurrentRowCallables<byte[]> callables = new ConcurrentRowCallables<byte[]>(threadPool);
    callables.addCallable(new TimeoutRowCallable(TABLENAME, ROW));
    callables.addCallable(new TimeoutRowCallable(TABLENAME, ANOTHER_ROW));
    callables.addCallable(new TimeoutRowCallable(TABLENAME, ANOTHER_FAMILY));
    callables.addCallable(new TimeoutRowCallable(TABLENAME, ANOTHER_QUALIFIER));
    callables.waitForResult();
    Assert.assertEquals(3, callables.getResults().size());
    Assert.assertTrue(callables.getException(TABLENAME, ANOTHER_QUALIFIER).getCause()
      instanceof RejectedExecutionException);
  }
  
  @Test
  public void testDuplicateRowCallable() throws IOException {
    ConcurrentRowCallables<byte[]> callables = new ConcurrentRowCallables<byte[]>(threadPool);
    callables.addCallable(new TimeoutRowCallable(TABLENAME, ROW));
    callables.addCallable(new TimeoutRowCallable(TABLENAME, ANOTHER_ROW));
    callables.addCallable(new TimeoutRowCallable(TABLENAME, ANOTHER_FAMILY));
    callables.addCallable(new TimeoutRowCallable(TABLENAME, ANOTHER_QUALIFIER));
    try {
      // duplicated with futures
      callables.addCallable(new TimeoutRowCallable(TABLENAME, ROW));
      Assert.fail();
    } catch (ThemisFatalException e) {}
    try {
      callables.addCallable(new TimeoutRowCallable(TABLENAME, ANOTHER_QUALIFIER));
      Assert.fail();
    } catch (ThemisFatalException e) {}
  }
}
