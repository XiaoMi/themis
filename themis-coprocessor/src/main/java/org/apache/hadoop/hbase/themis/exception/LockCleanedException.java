package org.apache.hadoop.hbase.themis.exception;

public class LockCleanedException extends ThemisException {
  private static final long serialVersionUID = 1006050325880182646L;

  public LockCleanedException(String msg) {
    super(msg);
  }
}
