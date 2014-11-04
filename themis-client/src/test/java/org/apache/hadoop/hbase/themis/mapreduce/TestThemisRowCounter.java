package org.apache.hadoop.hbase.themis.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestThemisRowCounter extends TransactionTestBase {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TransactionTestBase.setUpBeforeClass();
    if (TEST_UTIL != null) {
      TEST_UTIL.startMiniMapReduceCluster();
    }
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TransactionTestBase.tearDownAfterClass();
    if (TEST_UTIL != null) {
      TEST_UTIL.shutdownMiniMapReduceCluster();
    }
  }
  
  protected void writeTestData() throws IOException {
    Transaction transaction = new Transaction(connection);
    transaction.put(
      TABLENAME,
      new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE).add(ANOTHER_FAMILY, QUALIFIER, VALUE)
          .add(FAMILY, ANOTHER_FAMILY, VALUE));
    transaction.put(TABLENAME, new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, VALUE));
    transaction.commit();
  }
  
  @Test
  public void testThemisRowCounter() throws Exception {
    writeTestData();
    Job job = ThemisRowCounter.createSubmittableJob(conf, new String[] { Bytes.toString(TABLENAME),
        Bytes.toString(FAMILY) + ":" + Bytes.toString(QUALIFIER) });
    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());
    Counter counter = job.getCounters()
        .findCounter(ThemisRowCounter.RowCounterMapper.Counters.ROWS);
    assertEquals(2, counter.getValue());
    
    job = ThemisRowCounter.createSubmittableJob(conf, new String[] { Bytes.toString(TABLENAME),
        Bytes.toString(ANOTHER_FAMILY) + ":" + Bytes.toString(QUALIFIER) });
    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());
    counter = job.getCounters()
        .findCounter(ThemisRowCounter.RowCounterMapper.Counters.ROWS);
    assertEquals(1, counter.getValue());
  }
  
  @Test
  public void testThemisRowCounterWithoutQualifier() throws Exception {
    try {
      Job job = ThemisRowCounter.createSubmittableJob(conf,
        new String[] { Bytes.toString(TABLENAME), Bytes.toString(FAMILY) });
      job.waitForCompletion(true);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("must specify qualifier to read themis table"));
    }
  }
}
