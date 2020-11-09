package com.xiaomi.infra.themis;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadOnlyTransaction extends Transaction {
  public ReadOnlyTransaction(HConnection connection) throws IOException {
    super(connection.getConfiguration(), connection);
  }

  public ReadOnlyTransaction(HConnection connection, long startTs) throws IOException {
    super(connection.getConfiguration(), connection);
    this.startTs = startTs;
  }

  @Override
  public void commit() throws IOException {
    throw new IOException("Can not invoke commit in ReadOnlyTransaction");
  }

  public static int readColumnValue(Transaction transaction, ColumnCoordinate columnCoordinate) throws IOException {
    KeyValue kv = readColumn(transaction, columnCoordinate);
    return kv == null ? 0 : Bytes.toInt(kv.getValue());
  }

  public static KeyValue readColumn(Transaction transaction, ColumnCoordinate columnCoordinate) throws IOException {
    ThemisGet get = new ThemisGet(columnCoordinate.getRow()).addColumn(columnCoordinate.getFamily(),
      columnCoordinate.getQualifier());
    Result result = transaction.get(columnCoordinate.getTableName(), get);
    return result.isEmpty() ? null : result.list().get(0);
  }
  
  public long getStartTs() {
    return startTs;
  }
  
  public long getCommitTs() {
    return commitTs;
  }
}