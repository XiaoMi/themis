package com.xiaomi.infra.themis;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.themis.Transaction;

import com.xiaomi.infra.themis.exception.GeneratedTestException;

public class TransactionWithFail extends Transaction {
  private static final int FAIL_EVERY_OPERATION = 4000;
  private static Random random = new Random();
  
  public TransactionWithFail(Configuration conf, HConnection connection) throws IOException {
    super(conf, connection);
  }
  
  protected static Configuration createConfigUsingWriteInternalWithFail(Configuration conf) {
    Configuration overwriteConf = new Configuration(conf);
    return overwriteConf;
  }
  
  protected static void tryToGenerateFail(String methodName) throws GeneratedTestException {
    if (random.nextInt(FAIL_EVERY_OPERATION) == 0) {
      throw new GeneratedTestException("generate ioexception when invoke method=" + methodName);
    }
  }
  
  public void prewritePrimary() throws IOException {
    // TODO : throw exception after 'super.prewritePrimary'
    tryToGenerateFail("prewritePrimary");
    super.prewritePrimary();
  }

  public void prewriteSecondaries() throws IOException {
    tryToGenerateFail("prewriteSecondaries");
    super.prewriteSecondaries();
  }

  public void commitPrimary() throws IOException {
    tryToGenerateFail("commitPrimary");
    super.commitPrimary();
  }

  public void commitSecondaries() throws IOException {
    tryToGenerateFail("commitSecondaries");
    super.commitSecondaries();
  }
  
  public long getStartTs() {
    return startTs;
  }
  
  public long getCommitTs() {
    return commitTs;
  }
}
