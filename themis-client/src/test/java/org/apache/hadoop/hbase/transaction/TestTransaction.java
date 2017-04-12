package org.apache.hadoop.hbase.transaction;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.AggregateImplementation;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteEndpoint;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.regionserver.TestServerCustomProtocol;
import org.apache.hadoop.hbase.regionserver.ThemisRegionObserver;
import org.apache.hadoop.hbase.themis.ThemisTransaction;
import org.apache.hadoop.hbase.themis.ThemisTransactionTable;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpoint;
import org.apache.hadoop.hbase.themis.cp.ThemisScanObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.xiaomi.infra.hbase.client.HBaseClientInterface;

public class TestTransaction {
  public static final byte[] tableName = Bytes.toBytes("themis_table");
  public static final byte[] familyName = Bytes.toBytes("C");
  public static final byte[] qualifierName = Bytes.toBytes("Q");
  protected HBaseClientInterface client;
  private static HBaseTestingUtility util = null;
  private static MiniHBaseCluster cluster = null;
  private static TransactionService transactionService;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = HBaseConfiguration.create();
    conf.setStrings("hbase.coprocessor.user.region.classes", ThemisEndpoint.class.getName(),
      ThemisScanObserver.class.getName(), ThemisRegionObserver.class.getName());
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      AggregateImplementation.class.getName(), BulkDeleteEndpoint.class.getName(),
      MultiRowMutationEndpoint.class.getName(),
      TestServerCustomProtocol.PingHandler.class.getName(), ThemisScanObserver.class.getName());
    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, ThemisMasterObserver.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);

    util = new HBaseTestingUtility(conf);
    util.startMiniCluster();
    cluster = util.getMiniHBaseCluster();
    cluster.waitForActiveAndReadyMaster();
    createTestTable(conf);
    transactionService = TransactionService.getThemisTransactionService(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (transactionService != null) {
      transactionService.close();
    }
    util.shutdownMiniCluster();
    if (cluster != null) {
      cluster.waitUntilShutDown();
    }
  }

  public static HTableDescriptor getTestTableDesc() {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor family = new HColumnDescriptor(familyName);
    family.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, "true");
    desc.addFamily(family);
    return desc;
  }

  public static void createTestTable(Configuration conf) throws Exception {
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(getTestTableDesc());
    admin.close();
  }

  // Themis must be updated because raw limit change the method fingerprint of 
  // RegionScannerImpl's next method
  @Test
  public void testPutGetDeleteAndScan() throws Exception {
    // themis put
    Transaction transaction = new ThemisTransaction(transactionService);
    TransactionTable table = new ThemisTransactionTable(tableName, transaction);
    for (int i = 0; i < 5; ++i) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.add(familyName, qualifierName, Bytes.toBytes("value" + i));
      table.put(put);
    }
    transaction.commit();
    
    // themis get
    transaction = new ThemisTransaction(transactionService);
    table = new ThemisTransactionTable(tableName, transaction);
    for (int i = 0; i < 5; ++i) {
      Get get = new Get(Bytes.toBytes("row" + i));
      get.addColumn(familyName, qualifierName);
      Result result = table.get(get);
      Assert.assertEquals("value" + i, Bytes.toString(result.getValue(familyName, qualifierName)));
    }
    
    // themis scan
    Scan scan = new Scan();
    scan.addColumn(familyName, qualifierName);
    ResultScanner scanner = table.getScanner(scan);
    int count = 0;
    Result result = null;
    while ((result = scanner.next()) != null) {
      Assert.assertEquals("value" + count, Bytes.toString(result.getValue(familyName, qualifierName)));
      ++count;
    }
    scanner.close();
    Assert.assertEquals(5, count);
    
    List<Result> scanResults = table.scan(scan);
    Assert.assertEquals(5, scanResults.size());
    for (int i = 0; i < scanResults.size(); ++i) {
      Assert.assertEquals("value" + i,
        Bytes.toString(scanResults.get(i).getValue(familyName, qualifierName)));
    }
    
    scanResults = table.scan(scan, 3);
    Assert.assertEquals(3, scanResults.size());
    for (int i = 0; i < scanResults.size(); ++i) {
      Assert.assertEquals("value" + i,
        Bytes.toString(scanResults.get(i).getValue(familyName, qualifierName)));
    }
    
    // themis delete
    transaction = new ThemisTransaction(transactionService);
    table = new ThemisTransactionTable(tableName, transaction);
    for (int i = 0; i < 5; ++i) {
      Delete delete = new Delete(Bytes.toBytes("row" + i));
      delete.deleteColumn(familyName, qualifierName);
      table.delete(delete);
    }
    transaction.commit();
    
    // get after delete
    transaction = new ThemisTransaction(transactionService);
    table = new ThemisTransactionTable(tableName, transaction);
    for (int i = 0; i < 5; ++i) {
      Get get = new Get(Bytes.toBytes("row" + i));
      get.addColumn(familyName, qualifierName);
      result = table.get(get);
      Assert.assertTrue(result.isEmpty());
    }
    
    // scan after delete
    scan = new Scan();
    scan.addColumn(familyName, qualifierName);
    scanner = table.getScanner(scan);
    count = 0;
    result = null;
    while ((result = scanner.next()) != null) {
      ++count;
    }
    scanner.close();
    Assert.assertEquals(0, count);
    
    // test batch put
    transaction = new ThemisTransaction(transactionService);
    table = new ThemisTransactionTable(tableName, transaction);
    for (int i = 0; i < 5; ++i) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.add(familyName, qualifierName, Bytes.toBytes("value" + (i + 10)));
      table.put(put);
    }
    transaction.commit();
    
    // test batch get
    transaction = new ThemisTransaction(transactionService);
    table = new ThemisTransactionTable(tableName, transaction);
    List<Get> gets = new ArrayList<Get>();
    for (int i = 0; i < 5; ++i) {
      Get get = new Get(Bytes.toBytes("row" + i));
      get.addColumn(familyName, qualifierName);
      gets.add(get);
    }
    Result[] results = table.get(gets);
    for (int i = 0; i < 5; ++i) {
      Assert.assertEquals("value" + (i + 10),
        Bytes.toString(results[i].getValue(familyName, qualifierName)));
    }
  }
}
