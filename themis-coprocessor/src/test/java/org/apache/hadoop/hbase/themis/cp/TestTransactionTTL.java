package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.cp.TransactionTTL.TimestampType;
import org.junit.Assert;
import org.junit.Test;

public class TestTransactionTTL extends TestBase {
  @Test
  public void testToMs() throws IOException {
    TransactionTTL.init(HBaseConfiguration.create());
    long ms = TransactionTTL.toMs(369778447744761856l);
    Assert.assertTrue(ms < System.currentTimeMillis());
    Assert.assertTrue(ms > (System.currentTimeMillis() - 5l * 365 * 24 * 3600 * 1000));
  }
  
  @Test
  public void testGetExpiredTime() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    
    TimestampType type = TransactionTTL.timestampType;
    TransactionTTL.timestampType = TimestampType.MS;
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
    
    TransactionTTL.timestampType = TimestampType.CHRONOS;
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
    TransactionTTL.timestampType = type;
  }
  
  @Test
  public void testTypeEqual() {
    TimestampType type = TimestampType.valueOf(TimestampType.MS.toString());
    Assert.assertTrue(type == TimestampType.MS && type != TimestampType.CHRONOS);
    type = TimestampType.valueOf(TimestampType.CHRONOS.toString());
    Assert.assertTrue(type == TimestampType.CHRONOS && type != TimestampType.MS);
  }
}
