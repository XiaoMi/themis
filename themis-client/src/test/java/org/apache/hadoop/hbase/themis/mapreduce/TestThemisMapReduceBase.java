package org.apache.hadoop.hbase.themis.mapreduce;

import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestThemisMapReduceBase extends TransactionTestBase {
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
}
