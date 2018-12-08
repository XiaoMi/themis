package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@CoreCoprocessor
public class IndexMasterObserver implements MasterObserver, MasterCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(IndexMasterObserver.class);
  public static final String THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY = "SECONDARY_INDEX_NAMES";
  public static final byte[] THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY_BYTES =
    Bytes.toBytes(THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY);
  public static final String THEMIS_SECONDARY_INDEX_NAME_SPLITOR = ";";
  public static final String THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX = "__themis_index";
  public static final String THEMIS_SECONDARY_INDEX_TABLE_ATTRIBUTE_KEY = "THEMIS_SECONDARY_TABLE";
  public static final String THEMIS_SECONDARY_INDEX_TABLE_FAMILY = "I";
  public static final byte[] THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES =
    Bytes.toBytes(THEMIS_SECONDARY_INDEX_TABLE_FAMILY);

  private final ThreadLocal<TableDescriptor> deletedTableDesc = new ThreadLocal<>();

  private MasterServices services;

  @Override
  public void start(@SuppressWarnings("rawtypes") CoprocessorEnvironment env) throws IOException {
    assert env instanceof HasMasterServices;
    services = ((HasMasterServices) env).getMasterServices();
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableDescriptor desc, RegionInfo[] regions) throws IOException {
    if (desc.getTableName().getNameAsString()
      .startsWith(THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX)) {
      if (desc.getValue(THEMIS_SECONDARY_INDEX_TABLE_ATTRIBUTE_KEY) == null) {
        throw new IOException("table name prefix : '" + THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX +
          "' is preserved, invalid table name : " + desc.getTableName());
      }
      return;
    }
    List<ColumnFamilyDescriptor> secondaryIndexColumns = new ArrayList<>();
    for (ColumnFamilyDescriptor columnDesc : desc.getColumnFamilies()) {
      if (isSecondaryIndexEnableFamily(columnDesc)) {
        if (!ThemisMasterObserver.isThemisEnableTable(desc)) {
          throw new IOException("'" + THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY +
            "' must be set on themis-enabled family, invalid family=" + columnDesc);
        }
        secondaryIndexColumns.add(columnDesc);
      }
    }

    if (!secondaryIndexColumns.isEmpty()) {
      for (ColumnFamilyDescriptor secondaryIndexColumn : secondaryIndexColumns) {
        checkIndexNames(secondaryIndexColumn);
      }

      for (ColumnFamilyDescriptor secondaryIndexColumn : secondaryIndexColumns) {
        createSecondaryIndexTables(desc.getTableName(), secondaryIndexColumn);
      }
    }
  }

  protected static void checkIndexNames(ColumnFamilyDescriptor familyDesc) throws IOException {
    String indexAttribute =
      Bytes.toString(familyDesc.getValue(THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY_BYTES));
    String[] indexColumns = indexAttribute.split(THEMIS_SECONDARY_INDEX_NAME_SPLITOR);
    Set<String> indexColumnSet = new HashSet<String>();
    // check duplicate
    for (String indexColumn : indexColumns) {
      if (indexColumnSet.contains(indexColumn)) {
        throw new IOException("duplicate secondary index definition, indexAttribute=" +
          indexAttribute + ", familyDesc:" + familyDesc);
      }
      byte[][] indexNameAndColumn = CellUtil.parseColumn(Bytes.toBytes(indexColumn));
      if (indexNameAndColumn.length == 1 || indexNameAndColumn[0] == null ||
        indexNameAndColumn[0].length == 0 || indexNameAndColumn[1] == null ||
        indexNameAndColumn[1].length == 0) {
        throw new IOException(
          "illegal secondary index definition, please set index as 'indexName:qualifier', but is:" +
            indexColumn);
      }
      indexColumnSet.add(indexColumn);
    }
  }

  private boolean waitProcedureDone(long procId) throws InterruptedException {
    for (;;) {
      Procedure<?> proc = services.getMasterProcedureExecutor().getResultOrProcedure(procId);
      if (proc == null) {
        return true;
      }
      if (!proc.isFinished()) {
        Thread.sleep(1000);
        continue;
      }
      return proc.isSuccess();
    }
  }

  private void createSecondaryIndexTables(TableName tableName, ColumnFamilyDescriptor familyDesc)
      throws IOException {
    List<TableName> indexTableNames = getSecondaryIndexTableNames(tableName, familyDesc);
    if (indexTableNames.size() > 1) {
      throw new IOException(
        "currently, only allow to define one index on each column, but indexes are : " +
          indexTableNames);
    }
    for (TableName indexTableName : indexTableNames) {
      long procId = services.createTable(getSecondaryIndexTableDesc(indexTableName), null, -1,
        HConstants.NO_NONCE);
      try {
        if (!waitProcedureDone(procId)) {
          throw new IOException("Failed to create secondary index table:" + indexTableName);
        }
      } catch (InterruptedException e) {
        throw (IOException) new InterruptedIOException(
          "Interrupted while waiting for creating secondary index table:" + indexTableName)
            .initCause(e);
      }
      LOG.info("create secondary index table:" + indexTableName);
    }
  }

  protected static List<Pair<String, String>> getIndexNameAndColumns(TableName tableName,
      ColumnFamilyDescriptor familyDesc) {
    List<Pair<String, String>> indexNameAndColumns = new ArrayList<Pair<String, String>>();
    String indexAttribute =
      Bytes.toString(familyDesc.getValue(THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY_BYTES));
    String[] indexColumns = indexAttribute.split(THEMIS_SECONDARY_INDEX_NAME_SPLITOR);
    for (String indexColumn : indexColumns) {
      byte[][] indexNameAndColumn = CellUtil.parseColumn(Bytes.toBytes(indexColumn));
      String indexName = Bytes.toString(indexNameAndColumn[0]);
      String columnName = Bytes.toString(indexNameAndColumn[1]);
      indexNameAndColumns.add(new Pair<String, String>(indexName, columnName));
    }
    return indexNameAndColumns;
  }

  protected static List<TableName> getSecondaryIndexTableNames(TableName tableName,
      ColumnFamilyDescriptor familyDesc) {
    List<TableName> tableNames = new ArrayList<>();
    List<Pair<String, String>> indexNameAndColumns = getIndexNameAndColumns(tableName, familyDesc);
    for (Pair<String, String> indexNameAndColumn : indexNameAndColumns) {
      TableName indexTableName =
        constructSecondaryIndexTableName(tableName, familyDesc.getNameAsString(),
          indexNameAndColumn.getSecond(), indexNameAndColumn.getFirst());
      tableNames.add(indexTableName);
    }
    return tableNames;
  }

  protected static TableName constructSecondaryIndexTableName(TableName tableName,
      String familyName, String columnName, String indexName) {
    return TableName.valueOf(THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX + "_" +
      tableName.getNameAsString() + "_" + familyName + "_" + columnName + "_" + indexName);
  }

  protected static TableName constructSecondaryIndexTableName(TableName tableName,
      byte[] familyName, byte[] columnName, byte[] indexName) {
    return constructSecondaryIndexTableName(tableName, Bytes.toString(familyName),
      Bytes.toString(columnName), Bytes.toString(indexName));
  }

  protected static TableDescriptor getSecondaryIndexTableDesc(TableName tableName)
      throws IOException {
    // TODO : add split keys for index table
    return TableDescriptorBuilder.newBuilder(tableName)
      .setValue(THEMIS_SECONDARY_INDEX_TABLE_ATTRIBUTE_KEY, "true")
      .setColumnFamily(getSecondaryIndexFamily()).build();
  }

  protected static ColumnFamilyDescriptor getSecondaryIndexFamily() throws IOException {
    return ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes(THEMIS_SECONDARY_INDEX_TABLE_FAMILY))
      .setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString()).build();
  }

  protected static boolean isSecondaryIndexEnableTable(TableDescriptor desc) throws IOException {
    for (ColumnFamilyDescriptor familyDesc : desc.getColumnFamilies()) {
      if (isSecondaryIndexEnableFamily(familyDesc)) {
        return true;
      }
    }
    return false;
  }

  protected static boolean isSecondaryIndexEnableFamily(ColumnFamilyDescriptor desc)
      throws IOException {
    return desc.getValue(THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY_BYTES) != null;
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
      throws IOException {
    TableDescriptor tableDesc = services.getTableDescriptors().get(tableName);
    if (isSecondaryIndexEnableTable(tableDesc)) {
      LOG.info(
        "keep table desc for secondary index enable table, tableName=" + tableDesc.getTableName());
      deletedTableDesc.set(tableDesc);
    }
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    TableDescriptor tableDesc = deletedTableDesc.get();
    if (tableDesc != null) {
      deletedTableDesc.remove();
      for (ColumnFamilyDescriptor familyDesc : tableDesc.getColumnFamilies()) {
        if (isSecondaryIndexEnableFamily(familyDesc)) {
          List<TableName> indexTableNames =
            getSecondaryIndexTableNames(tableDesc.getTableName(), familyDesc);
          for (TableName indexTableName : indexTableNames) {
            services.disableTable(indexTableName, -1, HConstants.NO_NONCE);
            LOG.info("disabled index table name : " + indexTableName);
            long procId = services.deleteTable(indexTableName, -1, HConstants.NO_NONCE);
            try {
              if (!waitProcedureDone(procId)) {
                throw new IOException("Failed to delete secondary index table:" + indexTableName);
              }
            } catch (InterruptedException e) {
              throw (IOException) new InterruptedIOException(
                "Interrupted while waiting for deleting secondary index table:" + indexTableName)
                  .initCause(e);
            }
            LOG.info("deleted index table name : " + indexTableName);
          }
        }
      }
    }
  }
}
