package com.xiaomi.infra.themis.checker;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.apache.hadoop.hbase.themis.ThemisScan;
import org.apache.hadoop.hbase.themis.ThemisScanner;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import com.xiaomi.infra.themis.ReadOnlyTransaction;
import com.xiaomi.infra.themis.validator.Statistics;
import com.xiaomi.infra.themis.validator.Validator;

// 1. check the total value of columns not changed
// 2. check the values of given columns
public class ValueChecker extends Thread {
  private static final Log LOG = LogFactory.getLog(ValueChecker.class);
  
  public ValueChecker(Configuration conf) {}
  
  public static void addAllColumnsToScan(ThemisScan scan) throws IOException {
    for (int i = 0; i < Validator.families.length; ++i) {
      for (int j = 0; j < Validator.qualifiers.length; ++j) {
        scan.addColumn(Validator.families[i], Validator.qualifiers[j]);
      }
    }
  }
  
  public static void addAllColumnsToGet(ThemisGet get) throws IOException {
    for (int i = 0; i < Validator.families.length; ++i) {
      for (int j = 0; j < Validator.qualifiers.length; ++j) {
        get.addColumn(Validator.families[i], Validator.qualifiers[j]);
      }
    }
  }
  
  public static long getTotalValue(ReadOnlyTransaction transaction) throws IOException {
    long totalValue = 0;
    for (int i = 0; i < Validator.tableNames.length; ++i) {
      long startTs = System.currentTimeMillis();
      ThemisScanner scanner = null;
      try {
        ThemisScan pScan = new ThemisScan();
        addAllColumnsToScan(pScan);
        scanner = transaction.getScanner(Validator.tableNames[i], pScan);
        Result result = null;
        while ((result = scanner.next()) != null) {
          if (!result.isEmpty()) {
            for (KeyValue kv : result.list()) {
              totalValue += Bytes.toInt(kv.getValue());
            }
          }
        }
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
      Statistics.getStatistics().totalValueCheckLatency.update(System.currentTimeMillis() - startTs);
    }
    return totalValue;
  }
  
  public static long getTotalValueByGet(ReadOnlyTransaction transaction) throws IOException {
    long totalValue = 0;
    for (int i = 0; i < Validator.columnCoordinates.size(); ++i) {
      int value = ReadOnlyTransaction.readColumnValue(transaction, Validator.columnCoordinates.get(i));
      totalValue += value;
    }
    return totalValue;
  }
  
  @Override
  public void run() {
    Long startTs = null;
    while (true) {
      try {
        ReadOnlyTransaction transaction = new ReadOnlyTransaction(Validator.connection);
        startTs = transaction.getStartTs();
        long actualTotal = getTotalValue(transaction);
        if (actualTotal != Validator.totalValue) {
          LOG.fatal("total value check not match, actualTotal=" + actualTotal + ", expectTotal="
              + Validator.totalValue + ", startTs=" + startTs);
          System.exit(-1);
        } else {
          LOG.warn("check total value success, totalValue=" + actualTotal + ", startTs=" + startTs);
        }
        Threads.sleep(2000);
      } catch (IOException e) {
        LOG.error("read all columns fail when check total, startTs=" + startTs, e);
      } catch (Throwable e) {
        LOG.fatal("read all columns encounter other exception", e);
        System.exit(-1);
      }
    }
  }
  
  public static void checkUpdateValues(String workerName, ColumnCoordinate[] columns, Integer[] values,
      long commitTs) throws IOException {
    ReadOnlyTransaction transaction = new ReadOnlyTransaction(Validator.connection, commitTs + 1);
    for (int i = 0; i < columns.length; ++i) {
      int actualValue = ReadOnlyTransaction.readColumnValue(transaction, columns[i]);
      if (actualValue != values[i]) {
        LOG.error(workerName + " check updated value fail, column" + columns[i] + ", expectValue="
            + values[i] + ", actualValue=" + actualValue);
        System.exit(-1);
      }
    }
    LOG.info(workerName + " check update success");
  }
}
