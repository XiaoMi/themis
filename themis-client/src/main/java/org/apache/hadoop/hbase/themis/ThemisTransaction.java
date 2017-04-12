package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.transaction.NotSupportedException;
import org.apache.hadoop.hbase.transaction.Transaction;
import org.apache.hadoop.hbase.transaction.TransactionService;
import org.apache.hadoop.hbase.transaction.TransactionStatus;

import com.xiaomi.infra.hbase.client.HException;

public class ThemisTransaction extends Transaction {
  private static final Log LOG = LogFactory.getLog(ThemisTransaction.class);
  
  protected HConnection connection;
  protected org.apache.hadoop.hbase.themis.Transaction impl;

  public ThemisTransaction(TransactionService service) throws HException {
    super(service);
    try {
      connection = service.getHConnection();
      this.impl = new org.apache.hadoop.hbase.themis.Transaction(connection);
    } catch (IOException e) {
      throw new HException(e);
    }
  }

  @Override
  public void commit() throws HException {
    long startTs = System.currentTimeMillis();
    try {
      this.impl.commit();
    } catch (IOException e) {
      ThemisTransactionService.addFailCounter("themisCommit");
      throw new HException(e);
    } finally {
      long consumeInMs = System.currentTimeMillis() - startTs;
      ThemisTransactionService.logHBaseSlowAccess("themisCommit", consumeInMs);
      ThemisTransactionService.addCounter("themisCommit", consumeInMs);
    }
  }

  @Override
  public void rollback() throws HException {
    throw new HException(new NotSupportedException());
  }

  @Override
  public TransactionStatus getStatus() throws HException {
    throw new HException(new NotSupportedException());
  }

  @Override
  public Byte[] toByteArray() throws HException {
    throw new HException(new NotSupportedException());
  }

  @Override
  public Configuration getConf() {
    return impl.getConf();
  }
}
