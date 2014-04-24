package org.apache.hadoop.hbase.themis;

import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

public interface TransactionInterface {
  public Result get(byte[] tableName, ThemisGet get) throws IOException;
  public void put(byte[] tableName, ThemisPut put) throws IOException;
  public void delete(byte[] tableName, ThemisDelete delete) throws IOException;
  public ThemisScanner getScanner(byte[] tableName, ThemisScan scan) throws IOException;
  public void commit() throws IOException;
}
