package org.apache.hadoop.hbase.themis.exception;

import java.io.IOException;

// base exception class for exceptions of themis
public abstract class ThemisException extends IOException {
  private static final long serialVersionUID = -6176975655911849479L;

  public ThemisException(String msg) {
    super(msg);
  }
}
