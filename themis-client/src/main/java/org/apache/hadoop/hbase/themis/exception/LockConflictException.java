package org.apache.hadoop.hbase.themis.exception;

import org.apache.hadoop.hbase.themis.exception.ThemisException;

public class LockConflictException extends ThemisException {
  private static final long serialVersionUID = -3808041324929957463L;

  public LockConflictException(String msg) {
    super(msg);
  }
}