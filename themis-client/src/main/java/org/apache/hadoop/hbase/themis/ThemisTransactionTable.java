package org.apache.hadoop.hbase.themis;

import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.xiaomi.infra.hbase.client.HException;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.transaction.NotSupportedException;
import org.apache.hadoop.hbase.transaction.Transaction;
import org.apache.hadoop.hbase.transaction.TransactionTable;

public class ThemisTransactionTable extends TransactionTable {

  private ThemisTransaction themisTransaction;

  public ThemisTransactionTable(TableName tableName, Transaction transaction) throws HException {
    super(tableName, transaction);
    themisTransaction = (ThemisTransaction) this.transaction;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public Result get(Get get) throws IOException {
    long startTs = System.currentTimeMillis();
    try {
      // TODO : check get is legal for themis
      return themisTransaction.impl.get(tableName, new ThemisGet(get));
    } catch (IOException e) {
      ThemisTransactionService.addFailCounter("themisGet");
      throw e;
    } finally {
      long consumeInMs = System.currentTimeMillis() - startTs;
      ThemisTransactionService.logHBaseSlowAccess("themisGet", consumeInMs);
      ThemisTransactionService.addCounter("themisGet", consumeInMs);
    }
  }

  @Override
  public void put(Put put) throws IOException {
    // TODO : check put is legal for themis
    themisTransaction.impl.put(tableName, new ThemisPut(put));
  }

  @Override
  public void delete(Delete delete) throws IOException {
    // TODO : check put is legal for themis
    themisTransaction.impl.delete(tableName, new ThemisDelete(delete));
  }

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public TableDescriptor getDescriptor() throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    Result[] results = new Result[gets.size()];
    for (int i = 0; i < gets.size(); ++i) {
      results[i] = get(gets.get(i));
    }
    return results;
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    // TODO : check scan is legal for themis
    return themisTransaction.impl.getScanner(tableName, new ThemisScan(scan));
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    for (Put put : puts) {
      put(put);
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    for (Delete delete : deletes) {
      delete(delete);
    }
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable, Callback<R> callback) throws ServiceException, Throwable {
    throw new NotSupportedException();
  }
}
