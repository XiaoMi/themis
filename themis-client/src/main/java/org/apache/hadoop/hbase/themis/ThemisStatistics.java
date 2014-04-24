package org.apache.hadoop.hbase.themis;

import org.apache.hadoop.hbase.themis.cp.ThemisCpStatistics;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

// latency/counter statistics of key steps of themis client
public class ThemisStatistics implements Updater {
  private static final ThemisStatistics statistcs = new ThemisStatistics();
  private final MetricsRegistry registry = new MetricsRegistry();
  private final MetricsContext context;
  private final MetricsRecord metricsRecord;
  // metrics for prewrite/commit/random read/scan/rollback
  public final MetricsTimeVaryingRate prewriteLatency = new MetricsTimeVaryingRate("prewriteLatency", registry);
  public final MetricsTimeVaryingRate commitPrimaryLatency = new MetricsTimeVaryingRate("commitPrimaryLatency", registry);
  public final MetricsTimeVaryingRate commitSecondaryLatency = new MetricsTimeVaryingRate("commitSecondaryLatency", registry);
  public final MetricsTimeVaryingRate readLatency = new MetricsTimeVaryingRate("readLatency", registry);
  public final MetricsTimeVaryingRate getScannerLatency = new MetricsTimeVaryingRate("getScannerLatency", registry);
  public final MetricsTimeVaryingRate nextLatency = new MetricsTimeVaryingRate("nextLatency", registry);
  public final MetricsTimeVaryingLong rollbackCount = new MetricsTimeVaryingLong("rollbackCount", registry);
  // metrics for lock clean
  public final MetricsTimeVaryingRate cleanLockLatency = new MetricsTimeVaryingRate("cleanLockLatency", registry);
  public final MetricsTimeVaryingLong cleanLockSuccessCount = new MetricsTimeVaryingLong("cleanLockSuccessCount", registry);
  public final MetricsTimeVaryingLong cleanLockFailCount = new MetricsTimeVaryingLong("cleanLockFailCount", registry);
  public final MetricsTimeVaryingLong cleanLockByEraseCount = new MetricsTimeVaryingLong("cleanLockWithEraseCount", registry);
  public final MetricsTimeVaryingLong cleanLockByCommitCount = new MetricsTimeVaryingLong("cleanLockWithCommitCount", registry);
  // metrics for remote timestamp server 
  public final MetricsTimeVaryingRate batchSizeOfTimestampRequest = new MetricsTimeVaryingRate("batchSizeOfTimestampRequest", registry);
  public final MetricsTimeVaryingRate remoteTimestampRequestLatency = new MetricsTimeVaryingRate("remoteTimestampRequestLatency", registry);

  public ThemisStatistics() {
    context = MetricsUtil.getContext("themis");
    metricsRecord = MetricsUtil.createRecord(context, "client");
    context.registerUpdater(this);
  }
  
  public void doUpdates(MetricsContext context) {
    prewriteLatency.pushMetric(metricsRecord);
    commitPrimaryLatency.pushMetric(metricsRecord);
    commitSecondaryLatency.pushMetric(metricsRecord);
    readLatency.pushMetric(metricsRecord);
    nextLatency.pushMetric(metricsRecord);
    metricsRecord.update();
    rollbackCount.pushMetric(metricsRecord);
    cleanLockLatency.pushMetric(metricsRecord);
    cleanLockSuccessCount.pushMetric(metricsRecord);
    cleanLockFailCount.pushMetric(metricsRecord);
    cleanLockByEraseCount.pushMetric(metricsRecord);
    cleanLockByCommitCount.pushMetric(metricsRecord);
    getScannerLatency.pushMetric(metricsRecord);
    batchSizeOfTimestampRequest.pushMetric(metricsRecord);
    remoteTimestampRequestLatency.pushMetric(metricsRecord);
  }
  
  public static ThemisStatistics getStatistics() {
    return statistcs;
  }
  
  public static void updateLatency(MetricsTimeVaryingRate metric, long beginTs) {
    ThemisCpStatistics.updateLatency(metric, beginTs);
  }
}