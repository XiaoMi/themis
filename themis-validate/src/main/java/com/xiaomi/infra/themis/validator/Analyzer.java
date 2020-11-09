package com.xiaomi.infra.themis.validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.ThemisScan;
import org.apache.hadoop.hbase.themis.ThemisScanner;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;

import com.xiaomi.infra.themis.ReadOnlyTransaction;
import com.xiaomi.infra.themis.checker.ValueChecker;

public class Analyzer {
  private static final Log LOG = LogFactory.getLog(Analyzer.class);
  
  public static void findFirstErrorTransactionByUnmatchTotalValue(Configuration conf,
      long expectTotal, long upperStartTs) throws IOException {
    Validator.generateColumns();
    HConnection connection = Validator.createHConnection(conf);
    for (long startTs = upperStartTs; startTs > 2; --startTs) {
      ReadOnlyTransaction transaction = new ReadOnlyTransaction(connection, startTs);
      long actualTotal = ValueChecker.getTotalValue(transaction);
      LOG.warn("startTs=" + startTs + ", ExpectTotal=" + expectTotal + ", ActualTotal="
          + actualTotal);
      if (actualTotal == expectTotal) {
        System.exit(-1);
      }
    }
    connection.close();
  }
  
  public static void diffTransactions(Configuration conf, long startTs) throws IOException {
    diffTransactions(conf, startTs, startTs + 1);
  }
  
  public static void diffTransactions(Configuration conf, long srcStartTs,
      long dstStartTs) throws IOException {
    HConnection connection = Validator.createHConnection(conf);
    Validator.generateColumns();
    List<Integer> srcColumnValues = new ArrayList<Integer>();
    ReadOnlyTransaction transaction = new ReadOnlyTransaction(connection, srcStartTs);
    long srcTotalValue = 0;
    for (int i = 0; i < Validator.columnCoordinates.size(); ++i) {
      int value = ReadOnlyTransaction.readColumnValue(transaction, Validator.columnCoordinates.get(i));
      srcColumnValues.add(value);
      srcTotalValue += value;
    }

    transaction = new ReadOnlyTransaction(connection, dstStartTs);
    long dstTotalValue = 0;
    for (int i = 0; i < Validator.columnCoordinates.size(); ++i) {
      int dstValue = ReadOnlyTransaction.readColumnValue(transaction, Validator.columnCoordinates.get(i));
      dstTotalValue += dstValue;
      int srcValue = srcColumnValues.get(i).intValue();
      if (srcValue != dstValue) {
        LOG.warn("diff column=" + Validator.columnCoordinates.get(i) + ", srcValue=" + srcValue
            + ", dstValue=" + dstValue);
      }
    }
    LOG.warn("source total value=" + srcTotalValue + ", dst total value=" + dstTotalValue);
    connection.close();
  }
  
  public static void getTransactionColumns(Configuration conf, long commitTs) throws IOException {
    Validator.generateColumns();
    HConnection connection = HConnectionManager.createConnection(conf);
    for (int i = 0; i < Validator.columnCoordinates.size(); ++i) {
      ColumnCoordinate columnCoordinate = Validator.columnCoordinates.get(i);
      HTableInterface table = connection.getTable(columnCoordinate.getTableName());
      Column writeColumn = ColumnUtil.getPutColumn(columnCoordinate);
      Get get = new Get(columnCoordinate.getRow()).addColumn(writeColumn.getFamily(),
        writeColumn.getQualifier()).setTimeStamp(commitTs);
      Result result = table.get(get);
      if (!result.isEmpty()) {
        long startTs = Bytes.toLong(result.list().get(0).getValue());
        get = new Get(columnCoordinate.getRow()).addColumn(columnCoordinate.getFamily(), columnCoordinate.getQualifier())
            .setTimeStamp(startTs);
        long value = Bytes.toInt(table.get(get).list().get(0).getValue());
        LOG.warn("Transaction column, column=" + columnCoordinate + ", commitTs=" + commitTs + ", startTs="
            + Bytes.toLong(result.list().get(0).getValue()) + ", valueAfterTransaction=" + value);
      }
      table.close();
    }
    connection.close();
  }
  
  public static Map<ColumnCoordinate, Integer> readTotalValueofGivenTimestamp(HConnection connection, long timestamp)
      throws IOException {
    Map<ColumnCoordinate, Integer> columnValues = new HashMap<ColumnCoordinate, Integer>();
    long totalValue = 0;
    for (ColumnCoordinate columnCoordinate : Validator.columnCoordinates) {
      HTableInterface table = connection.getTable(columnCoordinate.getTableName());
      Get get = new Get(columnCoordinate.getRow()).addColumn(columnCoordinate.getFamily(), columnCoordinate.getQualifier());
      get.setTimeRange(timestamp, timestamp + 1);
      Result result = table.get(get);
      if (result.isEmpty()) {
        System.err.println("read empty result, column" + columnCoordinate);
      } else {
        int value = Bytes.toInt(result.list().get(0).getValue());
        totalValue += value;
        columnValues.put(columnCoordinate, value);
      }
      table.close();
    }
    System.out.println("Total value of timestamp=" + timestamp + ", value=" + totalValue);
    return columnValues;
  }
  
  public static Map<ColumnCoordinate, Integer> readValueByGet(HConnection connection, long timestamp)
      throws IOException {
    ReadOnlyTransaction transaction = new ReadOnlyTransaction(connection, timestamp);
    Map<ColumnCoordinate, Integer> columnValues = new HashMap<ColumnCoordinate, Integer>();
    for (int i = 0; i < Validator.columnCoordinates.size(); ++i) {
      int value = ReadOnlyTransaction.readColumnValue(transaction, Validator.columnCoordinates.get(i));
      columnValues.put(Validator.columnCoordinates.get(i), value);
    }
    System.out.println("Get count=" + columnValues.size());
    return columnValues;
  }
  
  public static void findDifferentValuesByGet(ReadOnlyTransaction transaction,
      Map<ColumnCoordinate, Integer> expectValues) throws IOException {
    long totalValue = 0;
    for (ColumnCoordinate columnCoordinate : Validator.columnCoordinates) {
      int expectValue = expectValues.get(columnCoordinate).intValue();
      int actualValue = ReadOnlyTransaction.readColumnValue(transaction, columnCoordinate);
      if (actualValue != expectValue) {
        System.err.println("unmatched value for column=" + columnCoordinate + ", expect="
            + expectValue + ", actual=" + actualValue);
      }
      totalValue += actualValue;
    }
    System.out.println("Total value=" + totalValue);
  }

  // help to find bug of scan
  public static void findDifferentValuesByScan(ReadOnlyTransaction transaction,
      Map<ColumnCoordinate, Integer> expectValues) throws IOException {
    long totalValue = 0;
    for (byte[] tableName : Validator.tableNames) {
      int readCellCount = 0;
      // TODO : reuse this with ValueChecker
      ThemisScanner scanner = null;
      try {
        ThemisScan pScan = new ThemisScan();
        ValueChecker.addAllColumnsToScan(pScan);
        scanner = transaction.getScanner(tableName, pScan);
        Result result = null;
        while ((result = scanner.next()) != null) {
          if (!result.isEmpty()) {
            for (KeyValue kv : result.list()) {
              ColumnCoordinate columnCoordinate = new ColumnCoordinate(tableName, kv.getRow(), kv.getFamily(), kv.getQualifier());
              int expectValue = expectValues.get(columnCoordinate).intValue();
              int actualValue = Bytes.toInt(kv.getValue());
              if (actualValue != expectValue) {
                System.err.println("unmatched value for column=" + columnCoordinate + ", expect="
                    + expectValue + ", actual=" + actualValue);
              }
              totalValue += actualValue;
              ++readCellCount;
            }
          }
        }
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
      System.out.println("readCellCount for table=" + Bytes.toString(tableName) + ", count="
          + readCellCount);
    }
    System.out.println("Total value=" + totalValue);
  }
  
  // TODO : add Column callable to resuse the code
  public static void main(String args[]) throws Exception {
    Configuration conf = Validator.initConfig();
    Validator.generateColumns();
    
    HConnection connection = Validator.createHConnection(conf);
    // do something to analyze the conflict error
//    ReadOnlyTransaction transaction = new ReadOnlyTransaction(connection, 35793);
//    System.out.println("TotalValue=" + ValueChecker.getTotalValue(transaction));
//    System.out.println("TotalValueByGet=" + ValueChecker.getTotalValueByGet(transaction));
//    readTotalValueofGivenTimestamp(connection, 35793);
//    findDifferentValuesByScan(transaction, readTotalValueofGivenTimestamp(connection, 1));
//    findDifferentValuesByScan(transaction, readValueByGet(connection, 35793));
//    findFirstErrorTransactionByUnmatchTotalValue(conf, 8219, 35793);
//    diffTransactions(conf, 35792);
    connection.close();
  }
}