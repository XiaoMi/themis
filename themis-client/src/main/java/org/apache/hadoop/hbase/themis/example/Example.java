package org.apache.hadoop.hbase.themis.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.ThemisDelete;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.ThemisScan;
import org.apache.hadoop.hbase.themis.ThemisScanner;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle.LocalTimestampOracle;
import org.apache.hadoop.hbase.util.Bytes;

public class Example {
  private static final byte[] TABLENAME = Bytes.toBytes("ThemisTable");
  private static final byte[] ROW = Bytes.toBytes("Row");
  private static final byte[] ANOTHER_ROW = Bytes.toBytes("AnotherRow");
  private static final byte[] FAMILY = Bytes.toBytes("ThemisCF");
  private static final byte[] QUALIFIER = Bytes.toBytes("Qualifier");
  private static final byte[] VALUE = Bytes.toBytes(10);
  private static Configuration conf;
  
  protected static void createTable(HConnection connection) throws IOException {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(connection);
      if (!admin.tableExists(TABLENAME)) {
        HTableDescriptor tableDesc = new HTableDescriptor(TABLENAME);
        HColumnDescriptor themisCF = new HColumnDescriptor(FAMILY);
        themisCF.setMaxVersions(Integer.MAX_VALUE);
        tableDesc.addFamily(themisCF);
        HColumnDescriptor lockCF = new HColumnDescriptor(ColumnUtil.LOCK_FAMILY_NAME);
        lockCF.setMaxVersions(1);
        lockCF.setInMemory(true);
        tableDesc.addFamily(lockCF);
        admin.createTable(tableDesc);
      } else {
        System.out.println(Bytes.toString(TABLENAME) + " exist, please check the schema of the table");
        if (!admin.isTableEnabled(TABLENAME)) {
          admin.enableTable(TABLENAME);
        }
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
  
  public static void main(String args[]) throws IOException {
    conf = HBaseConfiguration.create();
    HConnection connection = HConnectionManager.createConnection(conf);
    // will create 'ThemisTable' for test, the corresponding shell command is:
    // create 'ThemisTable', {NAME=>'ThemisCF', VERSIONS => '2147483647'}, {NAME => 'L', 'IN_MEMORY' => true, VERSIONS => '1'}
    createTable(connection);
    
    String timeStampOracleCls = conf.get(TransactionConstant.TIMESTAMP_ORACLE_CLASS_KEY,
      LocalTimestampOracle.class.getName());
    System.out.println("use timestamp oracle class : " + timeStampOracleCls);
    
    {
      // write two rows
      Transaction transaction = new Transaction(conf, connection);
      ThemisPut put = new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE);
      transaction.put(TABLENAME, put);
      put = new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, VALUE);
      transaction.put(TABLENAME, put);
      transaction.commit();
      System.out.println("init, using themisPut, set valueA=10, valueB=10");
    }
    
    {
      // read two rows and write new value
      Transaction transaction = new Transaction(conf, connection);
      ThemisGet get = new ThemisGet(ROW).addColumn(FAMILY, QUALIFIER);
      int valueA = Bytes.toInt(transaction.get(TABLENAME, get).getValue(FAMILY, QUALIFIER));
      get = new ThemisGet(ANOTHER_ROW).addColumn(FAMILY, QUALIFIER);
      int valueB = Bytes.toInt(transaction.get(TABLENAME, get).getValue(FAMILY, QUALIFIER));
      int delta = 5;
      valueA -= delta;
      valueB += delta;
      ThemisPut put = new ThemisPut(ROW).add(FAMILY, QUALIFIER, Bytes.toBytes(valueA));
      transaction.put(TABLENAME, put);
      put = new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, Bytes.toBytes(valueB));
      transaction.put(TABLENAME, put);
      transaction.commit();
      System.out.println("using themisPut, transfer 5 from ROW to ANOTHER_ROW");
    }
    
    {
      // check new value
      Transaction transaction = new Transaction(conf, connection);
      ThemisGet get = new ThemisGet(ROW).addColumn(FAMILY, QUALIFIER);
      int valueA = Bytes.toInt(transaction.get(TABLENAME, get).getValue(FAMILY, QUALIFIER));
      get = new ThemisGet(ANOTHER_ROW).addColumn(FAMILY, QUALIFIER);
      int valueB = Bytes.toInt(transaction.get(TABLENAME, get).getValue(FAMILY, QUALIFIER));
      System.out.println("after transffering, use themisGet, value of ROW is : " + valueA
          + ", value of ANOTHER_ROW is : " + valueB);
    }
    
    {
      // scan two rows
      Transaction transaction = new Transaction(conf, connection);
      ThemisScan scan = new ThemisScan();
      scan.addColumn(FAMILY, QUALIFIER);
      ThemisScanner scanner = transaction.getScanner(TABLENAME, scan);
      Result result = null;
      while ((result = scanner.next()) != null) {
        int value = Bytes.toInt(result.getValue(FAMILY, QUALIFIER));
        System.out.println("using themisScan, rowkey=" + Bytes.toString(result.getRow()) + ", value=" + value);
      }
      scanner.close();
    }
    
    {
      // delete one row
      Transaction transaction = new Transaction(conf, connection);
      ThemisDelete delete = new ThemisDelete(ROW);
      delete.deleteColumn(FAMILY, QUALIFIER);
      transaction.delete(TABLENAME, delete);
      transaction.commit();
      transaction = new Transaction(conf, connection);
      ThemisGet get = new ThemisGet(ROW).addColumn(FAMILY, QUALIFIER);
      Result result = transaction.get(TABLENAME, get);
      System.out.println("after delete, use themisGet, result of ROW isEmpty=" + result.isEmpty() + ", result=" + result);
    }
    
    connection.close();
    Transaction.destroy();
  }
}
