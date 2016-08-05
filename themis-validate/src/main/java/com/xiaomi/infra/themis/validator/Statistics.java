package com.xiaomi.infra.themis.validator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.hbase.metrics.histogram.MetricsTimeVaryingHistogram;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsFloatValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

public class Statistics implements Updater {
  private static final Log LOG = LogFactory.getLog(Statistics.class);
  private static final Statistics statistcs = new Statistics();
  private final MetricsRegistry registry = new MetricsRegistry();
  private final MetricsContext context;
  private final MetricsRecord metricsRecord;
  
  public final MetricsRate writeTransactionCount = new MetricsRate("writeTransactionCount", registry);
  public final MetricsRate successWriteCount = new MetricsRate("successWriteCount", registry);
  public final MetricsRate failWriteCount = new MetricsRate("failWriteCount", registry);
  public final MetricsRate lockConflictCount = new MetricsRate("lockConflictCount", registry);
  public final MetricsRate writeConflictCount = new MetricsRate("writeConflictCount", registry);
  public final MetricsRate deleteColumnCount = new MetricsRate("deleteColumnCount", registry);
  public final MetricsRate otherThemisFailCount = new MetricsRate("otherThemisFailCount", registry);
  public final MetricsRate hbaseFailCount = new MetricsRate("hbaseFailCount", registry);
  public final MetricsTimeVaryingInt wrokerExitCount = new MetricsTimeVaryingInt("wrokerExitCount", registry);
  
  public final MetricsTimeVaryingRate writeConflictCheckColumnCount = new MetricsTimeVaryingRate("writeConflictCheckColumnCount", registry);
  private final MetricsFloatValue transactionSuccessRate = new MetricsFloatValue("transactionSuccessRate", registry);
  
  public final MetricsTimeVaryingHistogram transactionWriteLatency = new MetricsTimeVaryingHistogram("transactionWriteLatency", registry);
  public final MetricsTimeVaryingHistogram transactionReadLatency = new MetricsTimeVaryingHistogram("transactionReadLatency", registry);
  public final MetricsTimeVaryingHistogram totalValueCheckLatency = new MetricsTimeVaryingHistogram("totalValueCheckLatency", registry);
  public final MetricsTimeVaryingHistogram writeConflictCheckLatency = new MetricsTimeVaryingHistogram("writeConflictCheckLatency", registry);
  
  public Statistics() {
    context = MetricsUtil.getContext("themis");
    metricsRecord = MetricsUtil.createRecord(context, "simulator");
    context.registerUpdater(this);
  }
  
  public void doUpdates(MetricsContext context) {
    LOG.warn("do updates for statistics");
    if (writeTransactionCount.getPreviousIntervalValue() != 0) {
      transactionSuccessRate.set(((float) successWriteCount.getPreviousIntervalValue())
          / writeTransactionCount.getPreviousIntervalValue());
    }
    writeTransactionCount.pushMetric(metricsRecord);
    successWriteCount.pushMetric(metricsRecord);
    transactionSuccessRate.pushMetric(metricsRecord);
    failWriteCount.pushMetric(metricsRecord);
    lockConflictCount.pushMetric(metricsRecord);
    writeConflictCount.pushMetric(metricsRecord);
    deleteColumnCount.pushMetric(metricsRecord);
    otherThemisFailCount.pushMetric(metricsRecord);
    hbaseFailCount.pushMetric(metricsRecord);
    writeConflictCheckColumnCount.pushMetric(metricsRecord);
    wrokerExitCount.pushMetric(metricsRecord);
    
    transactionReadLatency.pushMetric(metricsRecord);
    transactionWriteLatency.pushMetric(metricsRecord);
    totalValueCheckLatency.pushMetric(metricsRecord);
    writeConflictCheckLatency.pushMetric(metricsRecord);
    metricsRecord.update();
  }
  
  public static Statistics getStatistics() {
    return statistcs;
  }
  
  public static void main(String args[]) {
    Threads.sleep(1000000);
  }
}
