package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.transaction.NotSupportedException;
import org.apache.hadoop.hbase.transaction.Transaction;
import org.apache.hadoop.hbase.transaction.TransactionTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.xiaomi.infra.hbase.client.HException;

public class ThemisTransactionTable extends TransactionTable {
  private static final byte[] HBASE_NAMESERVICE_PREFIX = Bytes.toBytes("hbase://");
  private ThemisTransaction themisTransaction;

  public ThemisTransactionTable(byte[] tableName, Transaction transaction) throws HException {
    super(tableName, transaction);
    if (Bytes.startsWith(tableName, HBASE_NAMESERVICE_PREFIX)) {
      throw new HException("not support nameservice");
    }
    themisTransaction = (ThemisTransaction) this.transaction;
  }

  @Override
  public byte[] getTableName() {
    return tableName;
  }

  @Override
  public TableName getName() {
    return TableName.valueOf(tableName);
  }

  @Override
  public byte[] getFullTableName() {
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
  public HTableDescriptor getTableDescriptor() throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public Boolean[] exists(List<Get> gets) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException,
      InterruptedException {
    throw new NotSupportedException();
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    throw new NotSupportedException();
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    throw new NotSupportedException();
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Callback<R> callback)
      throws IOException, InterruptedException {
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
  public Result[] parallelGet(List<Get> gets) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    // TODO : check scan is legal for themis
    return themisTransaction.impl.getScanner(tableName, new ThemisScan(scan));
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    for (Put put : puts) {
      put(put);
    }
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    for (Delete delete : deletes) {
      delete(delete);
    }
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public Result append(Append append) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      Durability durability) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      boolean writeToWAL) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public boolean isAutoFlush() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void flushCommits() throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable) throws ServiceException, Throwable {
    throw new NotSupportedException();
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable, Callback<R> callback) throws ServiceException, Throwable {
    throw new NotSupportedException();
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
  }

  @Override
  public void setAutoFlushTo(boolean autoFlush) {

  }

  @Override
  public long getWriteBufferSize() {
    return 0;
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    throw new NotSupportedException();
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey,
      R responsePrototype) throws ServiceException, Throwable {
    throw new NotSupportedException();
  }

  @Override
  public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor,
      Message request, byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
      throws ServiceException, Throwable {
    throw new NotSupportedException();
  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, RowMutations mutation) throws IOException {
    throw new NotSupportedException();
  }
}
