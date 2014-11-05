package org.apache.hadoop.hbase.themis.cp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class TransactionTTL {
  public static final String THEMIS_TRANSACTION_TTL_ENABLE_KEY = "themis.transaction.ttl.enable";
  public static final String THEMIS_READ_TRANSACTION_TTL_KEY = "themis.read.transaction.ttl";
  public static final int DEFAULT_THEMIS_READ_TRANSACTION_TTL = 86400; // in second
  public static final String THEMIS_WRITE_TRANSACTION_TTL_KEY = "themis.write.transaction.ttl";
  public static final int DEFAULT_THEMIS_WRITE_TRANSACTION_TTL = 60; // in second
  public static final String THEMIS_TRANSACTION_TTL_TIME_ERROR_KEY = "themis.transaction.ttl.time.error";
  public static final int DEFAULT_THEMIS_TRANSACTION_TTL_TIME_ERROR = 10; // in second
  
  public static int readTransactionTTL;
  public static int writeTransactionTTL;
  public static int transactionTTLTimeError;
  public static TimestampType timestampType = TimestampType.CHRONOS;
  public static boolean transactionTTLEnable;
  
  public static enum TimestampType {
    CHRONOS,
    MS
  }
  
  public static void init(Configuration conf) throws IOException {
    transactionTTLEnable = conf.getBoolean(THEMIS_TRANSACTION_TTL_ENABLE_KEY, true);
    readTransactionTTL = conf.getInt(THEMIS_READ_TRANSACTION_TTL_KEY,
      DEFAULT_THEMIS_READ_TRANSACTION_TTL) * 1000;
    writeTransactionTTL = conf.getInt(THEMIS_WRITE_TRANSACTION_TTL_KEY,
      DEFAULT_THEMIS_WRITE_TRANSACTION_TTL) * 1000;
    transactionTTLTimeError = conf.getInt(THEMIS_TRANSACTION_TTL_TIME_ERROR_KEY,
      DEFAULT_THEMIS_TRANSACTION_TTL_TIME_ERROR) * 1000;
    if (readTransactionTTL < writeTransactionTTL + transactionTTLTimeError) {
      throw new IOException(
          "it is not reasonable to set readTransactionTTL just equal to writeTransactionTTL, readTransactionTTL="
              + readTransactionTTL
              + ", writeTransactionTTL="
              + writeTransactionTTL
              + ", transactionTTLTimeError=" + transactionTTLTimeError);
    }
  }
  
  // eg. 369778447744761856 >> 18
  public static long toMs(long themisTs) {
    return themisTs >> 18;
  }
  
  public static long toChronsTs(long ms) {
    return ms << 18;
  }
  
  public static long getExpiredTimestampForReadByCommitColumn(long currentMs) {
    if (timestampType == TimestampType.CHRONOS) {
      return getExpiredChronosForReadByCommitColumn(currentMs);
    } else if (timestampType == TimestampType.MS) {
      return getExpiredMsForReadByCommitColumn(currentMs);
    }
    return Long.MIN_VALUE;
  }
  
  private static long getExpiredMsForReadByCommitColumn(long currentMs) {
    return currentMs - readTransactionTTL - transactionTTLTimeError;
  }
  
  private static long getExpiredChronosForReadByCommitColumn(long currentMs) {
    return toChronsTs(getExpiredMsForReadByCommitColumn(currentMs));
  }
  
  public static long getExpiredTimestampForReadByDataColumn(long currentMs) {
    if (timestampType == TimestampType.CHRONOS) {
      return getExpiredChronosForReadByDataColumn(currentMs);
    } else if (timestampType == TimestampType.MS) {
      return getExpiredMsForReadByDataColumn(currentMs);
    }
    return Long.MIN_VALUE;
  }
  
  private static long getExpiredMsForReadByDataColumn(long currentMs) {
    return currentMs - readTransactionTTL - transactionTTLTimeError
        - writeTransactionTTL - transactionTTLTimeError;
  }
  
  private static long getExpiredChronosForReadByDataColumn(long currentMs) {
    return toChronsTs(getExpiredMsForReadByDataColumn(currentMs));
  }
  
  public static long getExpiredTimestampForWrite(long currentMs) {
    if (timestampType == TimestampType.CHRONOS) {
      return getExpiredChronosForWrite(currentMs);
    } else if (timestampType == TimestampType.MS) {
      return getExpiredMsForWrite(currentMs);
    }
    return Long.MIN_VALUE;
  }
  
  private static long getExpiredMsForWrite(long currentMs) {
    return currentMs - writeTransactionTTL - transactionTTLTimeError;
  }
  
  private static long getExpiredChronosForWrite(long currentMs) {
    return toChronsTs(getExpiredMsForWrite(currentMs));
  }
  
  public static boolean isLockExpired(long lockTs, long clientLockTTL) {
    long writeMs = (TransactionTTL.timestampType == TimestampType.MS ? lockTs : TransactionTTL
        .toMs(lockTs));
    return (System.currentTimeMillis() >= writeMs + clientLockTTL);
  }
}