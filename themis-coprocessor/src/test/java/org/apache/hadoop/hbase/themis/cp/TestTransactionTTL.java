package org.apache.hadoop.hbase.themis.cp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL.TimestampType;
import org.junit.Assert;
import org.junit.Test;

public class TestTransactionTTL extends TestBase {
  @Test
  public void testToMs() {
    TransactionTTL.init(HBaseConfiguration.create());
    long ms = TransactionTTL.toMs(369778447744761856l);
    Assert.assertTrue(ms < System.currentTimeMillis());
    Assert.assertTrue(ms > (System.currentTimeMillis() - 5l * 365 * 24 * 3600 * 1000));
  }
  
  @Test
  public void testGetExpiredTime() {
    Configuration conf = HBaseConfiguration.create();
    
    conf.set(TransactionTTL.THEMIS_TIMESTAMP_TYPE_KEY, TimestampType.MS.toString());
    TransactionTTL.init(conf);
    long ms = System.currentTimeMillis();
    long expiredTs = TransactionTTL.getExpiredTimestampForReadByCommitColumn(ms);
    Assert.assertEquals(ms, expiredTs + TransactionTTL.readTransactionTTL
        + TransactionTTL.transactionTTLTimeError);
    expiredTs = TransactionTTL.getExpiredTimestampForReadByDataColumn(ms);
    Assert.assertEquals(ms, expiredTs + TransactionTTL.readTransactionTTL
        + TransactionTTL.writeTransactionTTL + 2 * TransactionTTL.transactionTTLTimeError);
    expiredTs = TransactionTTL.getExpiredTimestampForWrite(ms);
    Assert.assertEquals(ms, expiredTs + TransactionTTL.writeTransactionTTL
        + TransactionTTL.transactionTTLTimeError);
    
    conf.set(TransactionTTL.THEMIS_TIMESTAMP_TYPE_KEY, TimestampType.CHRONOS.toString());
    TransactionTTL.init(conf);
    ms = System.currentTimeMillis();
    expiredTs = TransactionTTL.getExpiredTimestampForReadByCommitColumn(ms);
    Assert.assertEquals(
      ms,
      TransactionTTL.toMs(expiredTs) + TransactionTTL.readTransactionTTL
          + TransactionTTL.transactionTTLTimeError);
    expiredTs = TransactionTTL.getExpiredTimestampForReadByDataColumn(ms);
    Assert.assertEquals(
      ms,
      TransactionTTL.toMs(expiredTs) + TransactionTTL.readTransactionTTL
          + TransactionTTL.writeTransactionTTL + 2 * TransactionTTL.transactionTTLTimeError);
    expiredTs = TransactionTTL.getExpiredTimestampForWrite(ms);
    Assert.assertEquals(
      ms,
      TransactionTTL.toMs(expiredTs) + TransactionTTL.writeTransactionTTL
          + TransactionTTL.transactionTTLTimeError);
  }
}
