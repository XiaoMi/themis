package org.apache.hadoop.hbase.themis.exception;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.themis.ConcurrentRowCallables.TableAndRow;

public class MultiRowExceptions extends ThemisException {
  private static final long serialVersionUID = -5300909468331086844L;
  
  private Map<TableAndRow, IOException> exceptions;
  
  public MultiRowExceptions(String msg, Map<TableAndRow, IOException> exceptions) {
    super(msg + "\n" + constructMessage(exceptions));
    this.exceptions = exceptions;
  }

  public Map<TableAndRow, IOException> getExceptions() {
    return exceptions;
  }
  
  public static String constructMessage(Map<TableAndRow, IOException> exceptions) {
    StringBuilder message = new StringBuilder();
    for (Entry<TableAndRow, IOException> rowException : exceptions.entrySet()) {
      message.append("tableAndRow=")
              .append(rowException.getKey())
              .append(", exception=")
              .append(rowException.getValue())
              .append("\n");
    }
    return message.toString();
  }
}