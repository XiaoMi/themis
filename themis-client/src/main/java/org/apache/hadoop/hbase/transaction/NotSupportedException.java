package org.apache.hadoop.hbase.transaction;

public class NotSupportedException extends TransactionException {
  private static final long serialVersionUID = -8279995269466028446L;

  public NotSupportedException() {
  }

  public NotSupportedException(String msg) {
    super(msg);
  }
}
