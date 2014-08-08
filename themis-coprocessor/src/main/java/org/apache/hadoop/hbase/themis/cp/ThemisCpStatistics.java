package org.apache.hadoop.hbase.themis.cp;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

// latency statistics for key steps of themis coprocessor
public class ThemisCpStatistics implements Updater {
  private static final ThemisCpStatistics statistcs = new ThemisCpStatistics();
  private final MetricsRegistry registry = new MetricsRegistry();
  private final MetricsContext context;
  private final MetricsRecord metricsRecord;
  public final MetricsTimeVaryingRate getLockAndWriteLatency = new MetricsTimeVaryingRate("getLockAndWriteLatency", registry);
  public final MetricsTimeVaryingRate getDataLatency = new MetricsTimeVaryingRate("getDataLatency", registry);
  public final MetricsTimeVaryingRate prewriteReadLockLatency = new MetricsTimeVaryingRate("prewriteReadLockLatency", registry);
  public final MetricsTimeVaryingRate prewriteReadWriteLatency = new MetricsTimeVaryingRate("prewriteReadWriteLatency", registry);
  public final MetricsTimeVaryingRate prewriteWriteLatency = new MetricsTimeVaryingRate("prewriteWriteLatency", registry);
  public final MetricsTimeVaryingRate prewriteTotalLatency = new MetricsTimeVaryingRate("prewriteTotalLatency", registry); 
  public final MetricsTimeVaryingRate commitPrimaryReadLatency = new MetricsTimeVaryingRate("commitPrimaryReadLatency", registry);
  public final MetricsTimeVaryingRate commitWriteLatency = new MetricsTimeVaryingRate("commitWriteLatency", registry);
  public final MetricsTimeVaryingRate commitTotalLatency = new MetricsTimeVaryingRate("commitTotalLatency", registry);
  public final MetricsTimeVaryingRate getLockAndEraseReadLatency = new MetricsTimeVaryingRate("getLockAndEraseReadLatency", registry);
  public final MetricsTimeVaryingRate getLockAndEraseDeleteLatency = new MetricsTimeVaryingRate("getLockAndEraseDeleteLatency", registry);
  
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
    prewriteWriteLatency.pushMetric(metricsRecord);
    prewriteTotalLatency.pushMetric(metricsRecord);
    commitPrimaryReadLatency.pushMetric(metricsRecord);
    commitWriteLatency.pushMetric(metricsRecord);
    commitTotalLatency.pushMetric(metricsRecord);
    getLockAndEraseReadLatency.pushMetric(metricsRecord);
    getLockAndEraseDeleteLatency.pushMetric(metricsRecord);
    metricsRecord.update();
  }

  public static ThemisCpStatistics getThemisCpStatistics() {
    return statistcs;
  }
  
  public static void updateLatency(MetricsTimeVaryingRate metric, long beginTs) {
    metric.inc((System.nanoTime() - beginTs) / 1000);
  }
}
