package org.apache.hadoop.hbase.transaction;

public enum TransactionIsolationLevel {
  READ_UNCOMMITTED,
  READ_COMMITTED,
  REPEATABLE_READ,
  SERIALIZABLE;
}
