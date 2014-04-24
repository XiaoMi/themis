package org.apache.hadoop.hbase.themis.exception;

public class WriteConflictException extends ThemisException {
  private static final long serialVersionUID = -1897125427006232009L;

  public WriteConflictException(String msg) {
    super(msg);
  }
}
