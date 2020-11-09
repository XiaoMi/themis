package org.apache.hadoop.hbase.transaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.exception.HException;

public abstract class Transaction {
  protected TransactionService service;
  protected TransactionIsolationLevel isolation;
  protected TransactionType type;
  protected int timeout;

  public Transaction(TransactionService service) {
    this.service = service;
  }
  
  public abstract void commit() throws HException;

  public abstract void rollback() throws HException;

  public abstract TransactionStatus getStatus() throws HException;

  public abstract Byte[] toByteArray() throws HException;
  
  public abstract Configuration getConf();
}