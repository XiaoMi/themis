package org.apache.hadoop.hbase.transaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.themis.ThemisTransactionService;

import com.xiaomi.infra.hbase.client.HException;

public abstract class TransactionService {
  private static final Object lock = new Object();
  private static volatile TransactionService transactionService = null;
  protected Configuration config;

  public static final TransactionService getThemisTransactionService(Configuration conf)
      throws HException {
    return createSingletonThemisTransactionService(new Class<?>[] { Configuration.class },
      new Object[] { conf });
  }
  
  public static final TransactionService getThemisTransactionService(String configPath)
      throws HException {
    return createSingletonThemisTransactionService(new Class<?>[] { String.class },
      new Object[] { configPath });
  }
  
  protected static TransactionService createSingletonThemisTransactionService(Class<?>[] paraTypes,
      Object[] parameters) throws HException {
    try {
      if (transactionService == null) {
        synchronized (lock) {
          if (transactionService == null) {
            transactionService = ThemisTransactionService.class.getConstructor(paraTypes)
                .newInstance(parameters);
          }
        }
      }
      return transactionService;      
    } catch (Exception e) {
      throw new HException("create singleton transaction service fail", e);
    }
  }
  

  public TransactionService(Configuration config) {
    this.config = config;
  }
  public abstract Transaction[] getAll() throws HException;
  public abstract void setIsolationLevel(final TransactionIsolationLevel isolationLevel) throws HException;
  public abstract void setTransactionType(final TransactionType transactionType) throws HException;
  public abstract void setTransactionTimeout(final int timeoutSeconds) throws HException;
  public abstract TransactionIsolationLevel[] getSupportedIsolationLevels() throws HException;
  
  public abstract HConnection getHConnection();
  public void close() throws HException {};
}
