package org.apache.hadoop.hbase.themis.exception;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.themis.ConcurrentRowCallables.TableAndRow;
import org.apache.hadoop.hbase.themis.TestBase;
import org.junit.Test;

public class TestMultiRowExceptions extends TestBase {
  @Test
  public void testConstructMessage() {
    Map<TableAndRow, IOException> exceptions = new TreeMap<TableAndRow, IOException>();
    exceptions.put(new TableAndRow(TABLENAME, ROW), new IOException("exceptionA"));
    exceptions.put(new TableAndRow(TABLENAME, ANOTHER_ROW), new IOException("exceptionB"));
    System.out.println(MultiRowExceptions.constructMessage(exceptions));
  }
}
