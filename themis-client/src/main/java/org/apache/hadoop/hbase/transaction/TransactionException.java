package org.apache.hadoop.hbase.transaction;

import java.io.IOException;

public class TransactionException extends IOException {
  private static final long serialVersionUID = -6597435799399562922L;
  public TransactionException() {}
  public TransactionException(String msg) {
    super(msg);
  }
}
