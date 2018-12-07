package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;

public interface TransactionInterface {

  public Result get(TableName tableName, ThemisGet get) throws IOException;

  public void put(TableName tableName, ThemisPut put) throws IOException;

  public void delete(TableName tableName, ThemisDelete delete) throws IOException;

  public ThemisScanner getScanner(TableName tableName, ThemisScan scan) throws IOException;

  public void commit() throws IOException;
}
