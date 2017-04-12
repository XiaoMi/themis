package org.apache.hadoop.hbase.transaction;

import java.util.ArrayList;
import java.util.List;

import com.xiaomi.infra.hbase.client.InternalHBaseClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.themis.ThemisTransactionService;
import org.apache.hadoop.hbase.util.Bytes;

import com.xiaomi.infra.hbase.client.HException;

public abstract class TransactionTable implements HTableInterface {
  protected static final Log LOG = LogFactory.getLog(TransactionTable.class);
  protected Transaction transaction;
  protected byte[] tableName;
  protected int scannerCaching;
  
  public TransactionTable(String tableName, Transaction transaction) throws HException {
    this(Bytes.toBytes(tableName), transaction);
  }
  
  public TransactionTable(byte[] tableName, Transaction transaction) throws HException {
    this.tableName = tableName;
    this.transaction = transaction;
    this.scannerCaching = transaction.getConf().getInt(InternalHBaseClient.HBASE_CLIENT_SCANNER_CACHING, 1);
  }
  
  public List<Result> scan(Scan scan) throws HException {
    return scan(scan, Integer.MAX_VALUE);
  }

  public List<Result> scan(Scan scan, int limit) throws HException {
    long startTs = System.currentTimeMillis();
    List<Result> results = new ArrayList<Result>();
    ResultScanner scanner = null;
    try {
      int userSetCaching = scan.getCaching() <= 0 ? scannerCaching : scan.getCaching();
      if (userSetCaching > limit) {
        scan.setCaching(limit);
      }
      scanner = getScanner(scan);
      Result result = null;
      while ((result = scanner.next()) != null) {
        results.add(result);
        if (results.size() == limit) {
          break;
        }
      }
      return results;
    } catch (Throwable e) {
      ThemisTransactionService.addFailCounter("themisScan");
      throw new HException(e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      long consumeInMs = System.currentTimeMillis() - startTs;
      ThemisTransactionService.logHBaseSlowAccess("themisScan", consumeInMs);
      ThemisTransactionService.addCounter("themisScan", consumeInMs);
    }
  }
}
