package org.apache.hadoop.hbase.themis.cp;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.themis.TestBase;
import org.junit.Assert;
import org.junit.Test;

public class TestTransactionTTL extends TestBase {
  @Test
  public void testToMs() {
    TransactionTTL transactionTTL = new TransactionTTL(HBaseConfiguration.create());
    long ms = transactionTTL.toMs(369778447744761856l);
    Assert.assertTrue(ms < System.currentTimeMillis());
    Assert.assertTrue(ms > (System.currentTimeMillis() - 5l * 365 * 24 * 3600 * 1000));
  }
  
  @Test
  public void testGetExpiredTime() {
    long ms = System.currentTimeMillis();
    TransactionTTL transactionTTL = new TransactionTTL(HBaseConfiguration.create());
    long expiredTs = transactionTTL.getExpiredTsForReadByCommitColumn(ms);
    Assert.assertEquals(ms, transactionTTL.toMs(expiredTs) + transactionTTL.readTransactionTTL
        + transactionTTL.transactionTTLTimeError);
    expiredTs = transactionTTL.getExpiredTsForReadByDataColumn(ms);
    Assert.assertEquals(ms, transactionTTL.toMs(expiredTs) + transactionTTL.readTransactionTTL
      + transactionTTL.writeTransactionTTL + 2 * transactionTTL.transactionTTLTimeError);
    expiredTs = transactionTTL.getExpiredTsForWrite(ms);
    Assert.assertEquals(ms, transactionTTL.toMs(expiredTs) + transactionTTL.writeTransactionTTL
        + transactionTTL.transactionTTLTimeError);
  }
}
