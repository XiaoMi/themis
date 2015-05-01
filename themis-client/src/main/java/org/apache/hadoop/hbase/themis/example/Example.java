package org.apache.hadoop.hbase.themis.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.TransactionConstant;
import org.apache.hadoop.hbase.themis.timestamp.BaseTimestampOracle.LocalTimestampOracle;
import org.apache.hadoop.hbase.util.Bytes;

// This class shows an example of transfer $3 from Joe to Bob in cash table, where rows of Joe and Bob are
// located in different regions. The example will use the 'put' and 'get' APIs of Themis to do transaction.
public class Example {
  private static final byte[] CASHTABLE = Bytes.toBytes("CashTable"); // cash table
  private static final byte[] JOE = Bytes.toBytes("Joe"); // row for Joe
  private static final byte[] BOB = Bytes.toBytes("Bob"); // row for Bob
  private static final byte[][] splits = new byte[][]{Bytes.toBytes("C")};
  private static final byte[] FAMILY = Bytes.toBytes("Account");
  private static final byte[] CASH = Bytes.toBytes("cash");
  private static Configuration conf;
  
  protected static void createTable(HConnection connection) throws IOException {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(connection);
      if (!admin.tableExists(CASHTABLE)) {
        HTableDescriptor tableDesc = new HTableDescriptor(CASHTABLE);
        HColumnDescriptor themisCF = new HColumnDescriptor(FAMILY);
        // set THEMIS_ENABLE to allow Themis transaction on this family
        themisCF.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, "true");
        tableDesc.addFamily(themisCF);
        // the splits makes rows of Joe and Bob located in different regions
        admin.createTable(tableDesc, splits);
      } else {
        System.out.println(Bytes.toString(CASHTABLE)
            + " exist, won't create, please check the schema of the table");
        if (!admin.isTableEnabled(CASHTABLE)) {
          admin.enableTable(CASHTABLE);
        }
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }
  
  public static void main(String args[]) throws IOException {
    System.out.println("\n############################The Themis Example###########################\n");
    conf = HBaseConfiguration.create();
    HConnection connection = HConnectionManager.createConnection(conf);
    // will create 'CashTable' for test, the corresponding shell command is:
    // create 'CashTable', {NAME=>'ThemisCF', CONFIG => {'THEMIS_ENABLE', 'true'}}
    createTable(connection);
    
    String timeStampOracleCls = conf.get(TransactionConstant.TIMESTAMP_ORACLE_CLASS_KEY,
      LocalTimestampOracle.class.getName());
    System.out.println("use timestamp oracle class : " + timeStampOracleCls);
    
    {
      // initialize the accounts, Joe's cash is $20, Bob's cash is $9
      Transaction transaction = new Transaction(conf, connection);
      ThemisPut put = new ThemisPut(JOE).add(FAMILY, CASH, Bytes.toBytes(20));
      transaction.put(CASHTABLE, put);
      put = new ThemisPut(BOB).add(FAMILY, CASH, Bytes.toBytes(9));
      transaction.put(CASHTABLE, put);
      transaction.commit();
      System.out.println("initialize the accounts, Joe's account is $20, Bob's account is $9");
    }
    
    {
      // transfer $3 from Joe to Bob
      System.out.println("will transfer $3 from Joe to Bob");
      Transaction transaction = new Transaction(conf, connection);
      // firstly, read out the current cash for Joe and Bob
      ThemisGet get = new ThemisGet(JOE).addColumn(FAMILY, CASH);
      int cashOfJoe = Bytes.toInt(transaction.get(CASHTABLE, get).getValue(FAMILY, CASH));
      get = new ThemisGet(BOB).addColumn(FAMILY, CASH);
      int cashOfBob = Bytes.toInt(transaction.get(CASHTABLE, get).getValue(FAMILY, CASH));
      System.out.println("firstly, read out cash of the two users, Joe's cash is : $" + cashOfJoe
          + ", Bob's cash is : $" + cashOfBob);
      
      // then, transfer $3 from Joe to Bob, the mutations will be cached in client-side
      int transfer = 3;
      ThemisPut put = new ThemisPut(JOE).add(FAMILY, CASH, Bytes.toBytes(cashOfJoe - transfer));
      transaction.put(CASHTABLE, put);
      put = new ThemisPut(BOB).add(FAMILY, CASH, Bytes.toBytes(cashOfBob + transfer));
      transaction.put(CASHTABLE, put);
      // commit the mutations to server-side
      transaction.commit();
      System.out.println("then, transfer $3 from Joe to Bob and commit mutations to server-side");
      
      // lastly, read out the result after transferred
      transaction = new Transaction(conf, connection);
      get = new ThemisGet(JOE).addColumn(FAMILY, CASH);
      cashOfJoe = Bytes.toInt(transaction.get(CASHTABLE, get).getValue(FAMILY, CASH));
      get = new ThemisGet(BOB).addColumn(FAMILY, CASH);
      cashOfBob = Bytes.toInt(transaction.get(CASHTABLE, get).getValue(FAMILY, CASH));
      System.out.println("after cash transferred, read out cash of the two users, Joe's cash is : $" + cashOfJoe
          + ", Bob's cash is : $" + cashOfBob);
    }
    
    connection.close();
    Transaction.destroy();
    System.out.println("\n###############################Example End###############################");
  }
}
