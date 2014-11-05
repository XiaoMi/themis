package org.apache.hadoop.hbase.themis.exception;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class TransactionExpiredException extends DoNotRetryIOException {
  private static final long serialVersionUID = -7625657967701260552L;

  public TransactionExpiredException(String msg) {
    super(msg);
  }
}
