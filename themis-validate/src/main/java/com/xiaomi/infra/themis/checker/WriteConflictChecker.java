package com.xiaomi.infra.themis.checker;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.cache.Cache;
import com.xiaomi.infra.themis.ReadOnlyTransaction;
import com.xiaomi.infra.themis.validator.Statistics;
import com.xiaomi.infra.themis.validator.Validator;

public class WriteConflictChecker extends Thread {
  private static final Log LOG = LogFactory.getLog(WriteConflictChecker.class);
  private Map<ColumnCoordinate, Long> writeColumnCheckedTs = new ConcurrentHashMap<ColumnCoordinate, Long>();
  
  public WriteConflictChecker(Configuration conf) {
    for (int i = 0; i < Validator.columnCoordinates.size(); ++i) {
      ColumnCoordinate dataColumn = Validator.columnCoordinates.get(i);
      ColumnCoordinate writeColumn = new ColumnCoordinate(dataColumn.getTableName(), dataColumn.getRow(),
          ColumnUtil.getPutColumn(dataColumn));
      writeColumnCheckedTs.put(writeColumn, 0l);
    }
  }
  
  public void checkWriteConflict(ReadOnlyTransaction transaction) throws IOException {
    long totalKvCount = 0;
    for (Entry<ColumnCoordinate, Long> columnCheckedTs : writeColumnCheckedTs.entrySet()) {
      ColumnCoordinate columnCoordinate = columnCheckedTs.getKey();
      Column deleteColumn = ColumnUtil.getDeleteColumn(ColumnUtil.getDataColumn(columnCoordinate));
      long checkedTs = columnCheckedTs.getValue();
      Get get = new Get(columnCoordinate.getRow());
      get.addColumn(columnCoordinate.getFamily(), columnCoordinate.getQualifier());
      get.addColumn(deleteColumn.getFamily(), deleteColumn.getQualifier());
      get.setTimeRange(checkedTs, Long.MAX_VALUE);
      get.setMaxVersions();
      HTableInterface table = Validator.connection.getTable(columnCoordinate.getTableName());
      List<KeyValue> writeKvs = table.get(get).list();
      Collections.sort(writeKvs, new Comparator<KeyValue>() {
        @Override
        public int compare(KeyValue o1, KeyValue o2) {
          if (o1.getTimestamp() < o2.getTimestamp()) {
            return 1;
          } else if (o1.getTimestamp() > o2.getTimestamp()) {
            return -1;
          } else {
            LOG.fatal("encounter equal commitTs=" + o1.getTimestamp());
            System.exit(-1);
            return 0;
          }
        }
      });
      
      if (writeKvs != null) {
        totalKvCount += writeKvs.size();
        for (int i = 0; i < writeKvs.size() - 1; ++i) {
          long currentKvTs = writeKvs.get(i).getTimestamp();
          long currentKvPrewriteTs = Bytes.toLong(writeKvs.get(i).getValue());
          long nextKvTs = writeKvs.get(i + 1).getTimestamp();
          long nextKvPrewriteTs = Bytes.toLong(writeKvs.get(i + 1).getValue());
          // write conflict
          if (currentKvPrewriteTs <= nextKvTs) {
            LOG.fatal("discover write conflict, column=" + columnCoordinate + ", current ts=" + currentKvTs
                + ", current prewriteTs=" + currentKvPrewriteTs + ", next ts=" + nextKvTs
                + ", next prewriteTs=" + nextKvPrewriteTs);
            System.exit(-1);
          }
        }
        writeColumnCheckedTs.put(columnCoordinate, writeKvs.get(0).getTimestamp());
      }
      table.close();
    }
    Statistics.getStatistics().writeConflictCheckColumnCount.inc((int)(totalKvCount / Validator.columnCoordinates.size()));
  }
  
  @Override
  public void run() {
    int runCount = 0;
    while (true) {
      try {
        long startTs = System.currentTimeMillis();
        ReadOnlyTransaction transaction = new ReadOnlyTransaction(Validator.connection);
        checkWriteConflict(transaction);
        long consume = System.currentTimeMillis() - startTs;
        Statistics.getStatistics().writeConflictCheckLatency.update(consume);
        LOG.warn("check write conflits success, runCount=" + (++runCount) + ", consume=" + consume);
      } catch (IOException e) {
        LOG.error("encounter exception when checking write conflicts", e);
      } catch (Throwable e) {
        LOG.fatal("encounter other exception when checking write conflicts", e);
        System.exit(-1);
      }
      Threads.sleep(2000);
    }
  }
}