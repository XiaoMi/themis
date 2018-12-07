package org.apache.hadoop.hbase.themis.cp;

import com.google.protobuf.RpcCallback;
import java.io.IOException;
import java.io.InterruptedIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Simple {@link RpcCallback} implementation providing a {@link java.util.concurrent.Future}-like
 * {@link BlockingRpcCallback#get()} method, which will block util the instance's
 * {@link BlockingRpcCallback#run(Object)} method has been called. {@code R} is the RPC response
 * type that will be passed to the {@link #run(Object)} method.
 */
@InterfaceAudience.Private
public class BlockingRpcCallback<R> implements RpcCallback<R> {
  private R result;
  private boolean resultSet = false;

  /**
   * Called on completion of the RPC call with the response object, or {@code null} in the case of
   * an error.
   * @param parameter the response object or {@code null} if an error occurred
   */
  @Override
  public void run(R parameter) {
    synchronized (this) {
      result = parameter;
      resultSet = true;
      this.notifyAll();
    }
  }

  /**
   * Returns the parameter passed to {@link #run(Object)} or {@code null} if a null value was
   * passed. When used asynchronously, this method will block until the {@link #run(Object)} method
   * has been called.
   * @return the response object or {@code null} if no response was passed
   */
  public synchronized R get() throws IOException {
    while (!resultSet) {
      try {
        this.wait();
      } catch (InterruptedException ie) {
        InterruptedIOException exception = new InterruptedIOException(ie.getMessage());
        exception.initCause(ie);
        throw exception;
      }
    }
    return result;
  }
}
