package org.apache.hadoop.hbase.transaction;

public enum TransactionStatus {
  ACTIVE,
  COMMITTED,
  COMMITTING,
  NO_TRANSACTION,
  PREPARED,
  PREPARING,
  ROLLEDBACK,
  ROLLING_BACK;
}