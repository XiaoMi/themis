package com.xiaomi.infra.themis.validator;

import org.apache.hadoop.hbase.themis.TransactionConstant;


public class ValidatorConstant {
  public static final String TABLE_NAME_PREFIX = "ThemisSimulator";
  public static final String ROW_PREFIX = "r";
  public static final String FAMILY_PREFIX = "CF";
  public static final String QUALIFIER_PREFIX = "QF";
  public static final String VALIDATOR_TABLE_COUNT_KEY = "validator.table.count.key";
  public static final String VALIDATOR_ROW_COUNT_KEY = "validator.row.count.key";
  public static final String VALIDATOR_CF_COUNT_KEY = "validator.cf.count.key";
  public static final String VALIDATOR_QF_COUNT_KEY = "validator.qf.count.key";
  public static final String VALIDATOR_TRANSACTION_MAX_CELL_COUNT_KEY = "validator.transaction.max.cell.count";
  public static final String VALIDATOR_TABLE_REGION_COUNT = "validator.table.region.count";
  public static final String VALIDATOR_WORKER_COUNT_KEY = "validator.worker.count";
  public static final int MAX_COLUMN_INIT_VALUE = 100;
  public static final String VALIDATOR_ENABLE_TOTAL_VALUE_CHECKER = "validator.enable.totalvalue.checker";
  public static final String VALIDATOR_ENABLE_WRITE_CONFLCIT_CHECKER = "validator.enable.writeconflict.checker";
  public static final String VALIDATOR_INIT_USING_TRANSACTION = "validator.init.using.transaction";
  public static final String[] configKey = new String[] { "hbase.cluster.name",
      TransactionConstant.TIMESTAMP_ORACLE_CLASS_KEY,
      TransactionConstant.WORKER_REGISTER_CLASS_KEY, VALIDATOR_TABLE_COUNT_KEY,
      VALIDATOR_ROW_COUNT_KEY, VALIDATOR_CF_COUNT_KEY, VALIDATOR_QF_COUNT_KEY,
      VALIDATOR_TRANSACTION_MAX_CELL_COUNT_KEY, VALIDATOR_TABLE_REGION_COUNT,
      VALIDATOR_WORKER_COUNT_KEY, VALIDATOR_ENABLE_TOTAL_VALUE_CHECKER,
      VALIDATOR_ENABLE_WRITE_CONFLCIT_CHECKER, VALIDATOR_INIT_USING_TRANSACTION,
      TransactionConstant.THEMIS_ENABLE_CONCURRENT_RPC};
}
