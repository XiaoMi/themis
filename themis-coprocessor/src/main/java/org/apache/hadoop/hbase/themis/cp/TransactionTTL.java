package org.apache.hadoop.hbase.themis.cp;

import org.apache.hadoop.conf.Configuration;

public class TransactionTTL {
  public static final long MAX_TIMESTAMP_IN_MS = 4564200570578l; // 100 later since 2014.09.13
  public static final String THEMIS_READ_TRANSACTION_TTL_KEY = "themis.read.transaction.ttl";
  public static final int DEFAULT_THEMIS_READ_TRANSACTION_TTL = 86400; // in second
  public static final String THEMIS_WRITE_TRANSACTION_TTL_KEY = "themis.write.transaction.ttl";
  public static final int DEFAULT_THEMIS_WRITE_TRANSACTION_TTL = 60; // in second
  public static final String THEMIS_TRANSACTION_TTL_TIME_ERROR_KEY = "themis.transaction.ttl.time.error";
  public static final int DEFAULT_THEMIS_TRANSACTION_TTL_TIME_ERROR = 1; // in second
  
  public int readTransactionTTL;
  public int writeTransactionTTL;
  public int transactionTTLTimeError;
  
  public TransactionTTL(Configuration conf) {
    readTransactionTTL = conf.getInt(THEMIS_READ_TRANSACTION_TTL_KEY,
      DEFAULT_THEMIS_READ_TRANSACTION_TTL) * 1000;
    writeTransactionTTL = conf.getInt(THEMIS_WRITE_TRANSACTION_TTL_KEY,
      DEFAULT_THEMIS_WRITE_TRANSACTION_TTL) * 1000;
    transactionTTLTimeError = conf.getInt(THEMIS_TRANSACTION_TTL_TIME_ERROR_KEY,
      DEFAULT_THEMIS_TRANSACTION_TTL_TIME_ERROR) * 1000;
  }
  
  // eg. 369778447744761856 >> 18
  public long toMs(long themisTs) {
    return themisTs >> 18;
  }
  
  public long toThemisTs(long ms) {
    return ms << 18;
  }
  
  public long getExpiredMsForReadByCommitColumn(long currentMs) {
    return currentMs - readTransactionTTL - transactionTTLTimeError;
  }
  
  public long getExpiredTsForReadByCommitColumn(long currentMs) {
    return toThemisTs(getExpiredMsForReadByCommitColumn(currentMs));
  }
  
  public long getExpiredMsForReadByDataColumn(long currentMs) {
    return currentMs - readTransactionTTL - transactionTTLTimeError
        - writeTransactionTTL - transactionTTLTimeError;
  }
  
  public long getExpiredTsForReadByDataColumn(long currentMs) {
    return toThemisTs(getExpiredMsForReadByDataColumn(currentMs));
  }
  
  public long getExpiredMsForWrite(long currentMs) {
    return currentMs - writeTransactionTTL - transactionTTLTimeError;
  }
  
  public long getExpiredTsForWrite(long currentMs) {
    return toThemisTs(getExpiredMsForWrite(currentMs));
  }
}