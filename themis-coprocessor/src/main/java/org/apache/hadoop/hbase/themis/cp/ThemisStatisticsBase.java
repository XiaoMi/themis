package org.apache.hadoop.hbase.themis.cp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ThemisStatisticsBase implements Updater {
  private static final Logger LOG = LoggerFactory.getLogger(ThemisStatisticsBase.class);
  public static final String THEMIS_SLOW_OPERATION_CUTOFF_KEY = "themis.slow.operation.cutoff";
  public static final long DEFAULT_THEMIS_SLOW_OPERATION_CUTOFF = 100;
  protected static long slowCutoff = DEFAULT_THEMIS_SLOW_OPERATION_CUTOFF * 1000; // in us
  private static final String EmptySlowOperationMsg = "";

  public static void init(Configuration conf) {
    slowCutoff = conf.getLong(ThemisCpStatistics.THEMIS_SLOW_OPERATION_CUTOFF_KEY,
      ThemisCpStatistics.DEFAULT_THEMIS_SLOW_OPERATION_CUTOFF) * 1000;
  }

  public static void updateLatency(MetricsTimeVaryingRate metric, long beginTs, String message) {
    updateLatency(metric, beginTs, true, message);
  }

  public static void updateLatency(MetricsTimeVaryingRate metric, long beginTs, boolean logSlowOp) {
    updateLatency(metric, beginTs, logSlowOp, EmptySlowOperationMsg);
  }

  public static void updateLatency(MetricsTimeVaryingRate metric, long beginTs, boolean logSlowOp,
      String message) {
    long consumeInUs = (System.nanoTime() - beginTs) / 1000;
    metric.inc(consumeInUs);
    if (logSlowOp) {
      logSlowOperationInternal(metric.getName(), consumeInUs, message);
    }
  }

  public static void logSlowOperation(String operation, long beginTs, String message) {
    logSlowOperationInternal(operation, (System.nanoTime() - beginTs) / 1000, message);
  }

  public static void logSlowOperationInternal(String operation, long consumeInUs, String message) {
    if (consumeInUs > slowCutoff) {
      LOG.warn("themis slow operation " + operation + ", latency(ms)=" + (consumeInUs / 1000) +
        ", " + message);
    }
  }
}
