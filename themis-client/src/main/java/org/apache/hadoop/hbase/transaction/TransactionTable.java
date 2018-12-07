package org.apache.hadoop.hbase.transaction;

import com.xiaomi.infra.hbase.client.HConfigUtil;
import com.xiaomi.infra.hbase.client.HException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.themis.ThemisTransactionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TransactionTable implements Table {
  protected static final Logger LOG = LoggerFactory.getLogger(TransactionTable.class);
  protected Transaction transaction;
  protected TableName tableName;
  protected int scannerCaching;

  public TransactionTable(TableName tableName, Transaction transaction) throws HException {
    this.tableName = tableName;
    this.transaction = transaction;
    this.scannerCaching = transaction.getConf().getInt(HConfigUtil.HBASE_CLIENT_SCANNER_CACHING, 1);
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
