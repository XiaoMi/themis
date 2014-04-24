package org.apache.hadoop.hbase.themis.exception;

import org.apache.hadoop.hbase.themis.exception.ThemisException;

public class ThemisFatalException extends ThemisException {
  private static final long serialVersionUID = 5366776600868457401L;

  public ThemisFatalException(String msg) {
    super(msg);
  }
}
