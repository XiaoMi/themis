package org.apache.hadoop.hbase.themis.exception;

public class HException extends Exception {
  private static final long serialVersionUID = -3034994063807520421L;

  public HException() {
  }

  public HException(String message, Throwable cause) {
    super(message, cause);
  }

  public HException(String message) {
    super(message);
  }

  public HException(Throwable cause) {
    super(cause);
  }
}