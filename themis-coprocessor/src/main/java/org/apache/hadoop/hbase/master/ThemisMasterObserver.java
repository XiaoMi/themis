package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

public class ThemisMasterObserver extends BaseMasterObserver {
  private static final Log LOG = LogFactory.getLog(ThemisMasterObserver.class);

  public static final String THEMIS_TIMESTAMP_TYPE_KEY = "themis.timestamp.type";
  public static final String CHRONOS_TIMESTAMP = "chronos";
  public static final String MS_TIMESTAMP = "ms";
  public static final String THEMIS_EXPIRED_TIMESTAMP_CALCULATE_PERIOD_KEY = "themis.expired.timestamp.calculator.period";
  public static final String THEMIS_ENABLE_KEY = "THEMIS_ENABLE";
  
  private int expiredTsCalculatePeriod;
  private Chore themisExpiredTsCalculator;
  private TransactionTTL transactionTTL;
  
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    boolean themisEnable = false;
    for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
      if (isThemisEnableColumn(columnDesc)) {
        themisEnable = true;
        break;
      }
    }
    
    if (themisEnable) {
      for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
        if (Bytes.equals(ColumnUtil.LOCK_FAMILY_NAME, columnDesc.getName())) {
          throw new DoNotRetryIOException("family '" + ColumnUtil.LOCK_FAMILY_NAME_STRING
              + "' is preserved by themis when " + THEMIS_ENABLE_KEY
              + " is true, please change your family name");
        }
        // make sure TTL and MaxVersion is not set by user
        if (columnDesc.getTimeToLive() != HConstants.FOREVER) {
          throw new DoNotRetryIOException("can not set TTL for family '"
              + columnDesc.getNameAsString() + "' when " + THEMIS_ENABLE_KEY + " is true, TTL="
              + columnDesc.getTimeToLive());
        }
        if (columnDesc.getMaxVersions() != HColumnDescriptor.DEFAULT_VERSIONS
            && columnDesc.getMaxVersions() != Integer.MAX_VALUE) {
          throw new DoNotRetryIOException("can not set MaxVersion for family '"
              + columnDesc.getNameAsString() + "' when " + THEMIS_ENABLE_KEY
              + " is true, MaxVersion=" + columnDesc.getMaxVersions());
        }
        columnDesc.setMaxVersions(Integer.MAX_VALUE);
      }
      desc.addFamily(createLockFamily());
      LOG.info("add family '" + ColumnUtil.LOCK_FAMILY_NAME_STRING + "' for table:" + desc.getNameAsString());
    }    
  }
  
  protected static HColumnDescriptor createLockFamily() {
    HColumnDescriptor desc = new HColumnDescriptor(ColumnUtil.LOCK_FAMILY_NAME);
    desc.setInMemory(true);
    desc.setMaxVersions(1);
    desc.setTimeToLive(HConstants.FOREVER);
    // TODO(cuijianwei) : choose the best bloom filter type
    // desc.setBloomFilterType(BloomType.ROWCOL);
    return desc;
  }
  
  protected static boolean isThemisEnableColumn(HColumnDescriptor desc) {
    String value = desc.getValue(THEMIS_ENABLE_KEY);
    return value != null && Boolean.parseBoolean(value);
  }
  
  protected static boolean isThemisEnableTable(HTableDescriptor desc) {
    for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
      if (isThemisEnableColumn(columnDesc)) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    if (isThemisEnableTable(desc)) {
      startExpiredTimestampCalculator(ctx);
    }
  }
  
  protected synchronized void startExpiredTimestampCalculator(
      ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
    if (themisExpiredTsCalculator != null) {
      return;
    }

    LOG.info("try to start thread to compute the expired timestamp for themis table");
    String timestampType = ctx.getEnvironment().getConfiguration().get(THEMIS_TIMESTAMP_TYPE_KEY, CHRONOS_TIMESTAMP);
    if (!timestampType.equals(CHRONOS_TIMESTAMP) && !timestampType.equals(MS_TIMESTAMP)) {
      throw new IOException("timestamp type must be '" + CHRONOS_TIMESTAMP + "' or '"
          + MS_TIMESTAMP + "', but is '" + timestampType + "'");
    }
    transactionTTL = new TransactionTTL(ctx.getEnvironment().getConfiguration());
    String expiredTsCalculatePeriodStr = ctx.getEnvironment().getConfiguration()
        .get(THEMIS_EXPIRED_TIMESTAMP_CALCULATE_PERIOD_KEY);
    if (expiredTsCalculatePeriodStr == null) {
      expiredTsCalculatePeriod = transactionTTL.readTransactionTTL;
    } else {
      expiredTsCalculatePeriod = Integer.parseInt(expiredTsCalculatePeriodStr);
    }
    
    themisExpiredTsCalculator = new ExpiredTimestampCalculator(transactionTTL, timestampType,
        expiredTsCalculatePeriod * 1000, ctx.getEnvironment().getMasterServices());
    // TODO : should stop this thread when all themis-enable table deleted?
    Threads.setDaemonThreadRunning(themisExpiredTsCalculator.getThread());
    LOG.info("successfully start thread to compute the expired timestamp for themis table, timestampType="
        + timestampType + ", calculatorPeriod=" + expiredTsCalculatePeriod);
  }
  
  public static class ExpiredTimestampCalculator extends Chore {
    private long currentExpiredTs;
    private final String timestampType;
    private final TransactionTTL transactionTTL;
    
    public ExpiredTimestampCalculator(TransactionTTL transactionTTL, String timestampType, int p,
        Stoppable stopper) {
      super("ThemisExpiredTimestampCalculator", p, stopper);
      this.transactionTTL = transactionTTL;
      this.timestampType = timestampType;
    }

    @Override
    protected void chore() {
      long currentMs = System.currentTimeMillis();
      if (timestampType.equals(CHRONOS_TIMESTAMP)) {
        currentExpiredTs = transactionTTL.getExpiredTsForReadByDataColumn(currentMs);
      } else {
        currentExpiredTs = transactionTTL.getExpiredMsForReadByDataColumn(currentMs);
      }
      
      LOG.info("computed expired timestamp for themis, currentExpiredTs=" + currentExpiredTs);
      
      // TODO : write data to zookeeper
    }
    
    public long getCurrentExpiredTs() {
      return currentExpiredTs;
    }
  }
}
