package org.apache.hadoop.hbase.themis.cp;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

// latency statistics for key steps of themis coprocessor
public class ThemisCpStatistics extends ThemisStatisticsBase {
  private static final ThemisCpStatistics statistcs = new ThemisCpStatistics();
  private final MetricsRegistry registry = new MetricsRegistry();
  private final MetricsContext context;
  private final MetricsRecord metricsRecord;
  public final MetricsTimeVaryingRate getLockAndWriteLatency = new MetricsTimeVaryingRate("getLockAndWriteLatency", registry);
  public final MetricsTimeVaryingRate getDataLatency = new MetricsTimeVaryingRate("getDataLatency", registry);
  public final MetricsTimeVaryingRate prewriteReadLockLatency = new MetricsTimeVaryingRate("prewriteReadLockLatency", registry);
  public final MetricsTimeVaryingRate prewriteReadWriteLatency = new MetricsTimeVaryingRate("prewriteReadWriteLatency", registry);
  public final MetricsTimeVaryingRate prewriteCheckConflictRowLatency = new MetricsTimeVaryingRate(
      "prewriteCheckConflictRowLatency", registry);
  public final MetricsTimeVaryingRate prewriteWriteLatency = new MetricsTimeVaryingRate("prewriteWriteLatency", registry);
  public final MetricsTimeVaryingRate prewriteTotalLatency = new MetricsTimeVaryingRate("prewriteTotalLatency", registry); 
  public final MetricsTimeVaryingRate commitPrimaryReadLatency = new MetricsTimeVaryingRate("commitPrimaryReadLatency", registry);
  public final MetricsTimeVaryingRate commitWriteLatency = new MetricsTimeVaryingRate("commitWriteLatency", registry);
  public final MetricsTimeVaryingRate commitTotalLatency = new MetricsTimeVaryingRate("commitTotalLatency", registry);
  public final MetricsTimeVaryingRate getLockAndEraseReadLatency = new MetricsTimeVaryingRate("getLockAndEraseReadLatency", registry);
  public final MetricsTimeVaryingRate getLockAndEraseDeleteLatency = new MetricsTimeVaryingRate("getLockAndEraseDeleteLatency", registry);
  
  // metrics for lock clean
  public final MetricsTimeVaryingRate cleanLockLatency = new MetricsTimeVaryingRate("cleanLockLatency", registry);
  public final MetricsTimeVaryingLong cleanLockSuccessCount = new MetricsTimeVaryingLong("cleanLockSuccessCount", registry);
  public final MetricsTimeVaryingLong cleanLockFailCount = new MetricsTimeVaryingLong("cleanLockFailCount", registry);
  public final MetricsTimeVaryingLong cleanLockByEraseCount = new MetricsTimeVaryingLong("cleanLockWithEraseCount", registry);
  public final MetricsTimeVaryingLong cleanLockByCommitCount = new MetricsTimeVaryingLong("cleanLockWithCommitCount", registry);
  
  public ThemisCpStatistics() {
    context = MetricsUtil.getContext("themis");
    metricsRecord = MetricsUtil.createRecord(context, "coprocessor");
    context.registerUpdater(this);
  }
  
  public void doUpdates(MetricsContext context) {
    getLockAndWriteLatency.pushMetric(metricsRecord);
    getDataLatency.pushMetric(metricsRecord);
    prewriteReadLockLatency.pushMetric(metricsRecord);
    prewriteReadWriteLatency.pushMetric(metricsRecord);
    prewriteCheckConflictRowLatency.pushMetric(metricsRecord);
    prewriteWriteLatency.pushMetric(metricsRecord);
    prewriteTotalLatency.pushMetric(metricsRecord);
    commitPrimaryReadLatency.pushMetric(metricsRecord);
    commitWriteLatency.pushMetric(metricsRecord);
    commitTotalLatency.pushMetric(metricsRecord);
    getLockAndEraseReadLatency.pushMetric(metricsRecord);
    getLockAndEraseDeleteLatency.pushMetric(metricsRecord);
    cleanLockLatency.pushMetric(metricsRecord);
    cleanLockSuccessCount.pushMetric(metricsRecord);
    cleanLockFailCount.pushMetric(metricsRecord);
    cleanLockByEraseCount.pushMetric(metricsRecord);
    cleanLockByCommitCount.pushMetric(metricsRecord);
    metricsRecord.update();
  }

  public static ThemisCpStatistics getThemisCpStatistics() {
    return statistcs;
  }  
}
