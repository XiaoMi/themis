package org.apache.hadoop.hbase.themis.exception;

public class TransactionExpiredException extends ThemisException {
  private static final long serialVersionUID = -7625657967701260552L;

  public TransactionExpiredException(String msg) {
    super(msg);
  }
}
