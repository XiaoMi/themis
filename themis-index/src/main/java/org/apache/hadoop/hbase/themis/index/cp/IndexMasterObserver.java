package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class IndexMasterObserver extends BaseMasterObserver {
  private static final Log LOG = LogFactory.getLog(IndexMasterObserver.class);
  public static final String THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY = "SECONDARY_INDEX_NAMES";
  public static final String THEMIS_SECONDARY_INDEX_NAME_SPLITOR = ";";
  public static final String THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX = "__themis_index";
  public static final String THEMIS_SECONDARY_INDEX_TABLE_ATTRIBUTE_KEY = "THEMIS_SECONDARY_TABLE";
  public static final String THEMIS_SECONDARY_INDEX_TABLE_FAMILY = "I";
  public static final byte[] THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES = Bytes.toBytes(THEMIS_SECONDARY_INDEX_TABLE_FAMILY);
  
  private ThreadLocal<HTableDescriptor> deletedTableDesc = new ThreadLocal<HTableDescriptor>();
  
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    if (desc.getNameAsString().startsWith(THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX)) {
      if (desc.getValue(THEMIS_SECONDARY_INDEX_TABLE_ATTRIBUTE_KEY) == null) {
        throw new IOException("table name prefix : '" + THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX
            + "' is preserved, invalid table name : " + desc.getNameAsString());
      }
      return;
    }
    
    List<HColumnDescriptor> secondaryIndexColumns = new ArrayList<HColumnDescriptor>();
    for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
      if (isSecondaryIndexEnableFamily(columnDesc)) {
        if (!ThemisMasterObserver.isThemisEnableTable(desc)) {
          throw new IOException("'" + THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY
              + "' must be set on themis-enabled family, invalid family=" + columnDesc);
        }
        secondaryIndexColumns.add(columnDesc);
      }
    }
    
    if (secondaryIndexColumns.size() != 0) {
      for (HColumnDescriptor secondaryIndexColumn : secondaryIndexColumns) {
        checkIndexNames(secondaryIndexColumn);
      }

      HBaseAdmin admin = new HBaseAdmin(ctx.getEnvironment().getConfiguration());
      try {
        for (HColumnDescriptor secondaryIndexColumn : secondaryIndexColumns) {
          createSecondaryIndexTables(admin, desc.getNameAsString(), secondaryIndexColumn);
        }
      } finally {
        admin.close();
      }
    }
  }

  protected static void checkIndexNames(HColumnDescriptor familyDesc) throws IOException {
    String indexAttribute = familyDesc.getValue(THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY);
    String[] indexColumns = indexAttribute.split(THEMIS_SECONDARY_INDEX_NAME_SPLITOR);
    Set<String> indexColumnSet = new HashSet<String>();
    // check duplicate
    for (String indexColumn : indexColumns) {
      if (indexColumnSet.contains(indexColumn)) {
        throw new IOException("duplicate secondary index definition, indexAttribute="
            + indexAttribute + ", familyDesc:" + familyDesc);
      }
      byte[][] indexNameAndColumn = KeyValue.parseColumn(Bytes.toBytes(indexColumn));
      if (indexNameAndColumn.length == 1 || indexNameAndColumn[0] == null
          || indexNameAndColumn[0].length == 0 || indexNameAndColumn[1] == null
          || indexNameAndColumn[1].length == 0) {
        throw new IOException(
            "illegal secondary index definition, please set index as 'indexName:qualifier', but is:"
                + indexColumn);
      }
      indexColumnSet.add(indexColumn);
    }
  }
  
  protected void createSecondaryIndexTables(HBaseAdmin admin, String tableName,
      HColumnDescriptor familyDesc) throws IOException {
    List<String> indexTableNames = getSecondaryIndexTableNames(tableName, familyDesc);
    if (indexTableNames.size() > 1) {
      throw new IOException(
          "currently, only allow to define one index on each column, but indexes are : "
              + indexTableNames);
    }
    for (String indexTableName : indexTableNames) {
      admin.createTable(getSecondaryIndexTableDesc(indexTableName));
      LOG.info("create secondary index table:" + indexTableName);      
    }
  }
  
  protected static List<Pair<String, String>> getIndexNameAndColumns(String tableName,
      HColumnDescriptor familyDesc) {
    List<Pair<String, String>> indexNameAndColumns = new ArrayList<Pair<String,String>>();
    String indexAttribute = familyDesc.getValue(THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY);
    String[] indexColumns = indexAttribute.split(THEMIS_SECONDARY_INDEX_NAME_SPLITOR);
    for (String indexColumn : indexColumns) {
      byte[][] indexNameAndColumn = KeyValue.parseColumn(Bytes.toBytes(indexColumn));
      String indexName = Bytes.toString(indexNameAndColumn[0]);
      String columnName = Bytes.toString(indexNameAndColumn[1]);
      indexNameAndColumns.add(new Pair<String, String>(indexName, columnName));
    }
    return indexNameAndColumns;
  }
  
  protected static List<String> getSecondaryIndexTableNames(String tableName, HColumnDescriptor familyDesc) {
    List<String> tableNames = new ArrayList<String>();
    List<Pair<String, String>> indexNameAndColumns = getIndexNameAndColumns(tableName, familyDesc);
    for (Pair<String, String> indexNameAndColumn : indexNameAndColumns) {
      String indexTableName = constructSecondaryIndexTableName(tableName,
        familyDesc.getNameAsString(), indexNameAndColumn.getSecond(), indexNameAndColumn.getFirst());
      tableNames.add(indexTableName);
    }
    return tableNames;
  }
  
  protected static String constructSecondaryIndexTableName(String tableName, String familyName,
      String columnName, String indexName) {
    return THEMIS_SECONDARY_INDEX_TABLE_NAME_PREFIX + "_" + tableName + "_" + familyName + "_"
        + columnName + "_" + indexName;
  }
  
  protected static HTableDescriptor getSecondaryIndexTableDesc(String tableName) throws IOException {
    // TODO : add split keys for index table
    HTableDescriptor indexTableDesc = new HTableDescriptor(tableName);
    indexTableDesc.setValue(THEMIS_SECONDARY_INDEX_TABLE_ATTRIBUTE_KEY, "true");
    indexTableDesc.addFamily(getSecondaryIndexFamily());
    return indexTableDesc;
  }
  
  protected static HColumnDescriptor getSecondaryIndexFamily() throws IOException {
    HColumnDescriptor desc = new HColumnDescriptor(THEMIS_SECONDARY_INDEX_TABLE_FAMILY);
    desc.setValue(ThemisMasterObserver.THEMIS_ENABLE_KEY, Boolean.TRUE.toString());
    return desc;
  }
  
  protected static boolean isSecondaryIndexEnableTable(HTableDescriptor desc) throws IOException {
    for (HColumnDescriptor familyDesc : desc.getColumnFamilies()) {
      if (isSecondaryIndexEnableFamily(familyDesc)) {
        return true;
      }
    }
    return false;
  }
  
  protected static boolean isSecondaryIndexEnableFamily(HColumnDescriptor desc) throws IOException {
    return desc.getValue(THEMIS_SECONDARY_INDEX_FAMILY_ATTRIBUTE_KEY) != null;
  }
  
  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException {
    HTableDescriptor tableDesc = ctx.getEnvironment().getMasterServices().getTableDescriptors()
        .get(tableName);
    if (isSecondaryIndexEnableTable(tableDesc)) {
      LOG.info("keep table desc for secondary index enable table, tableName=" + tableDesc.getNameAsString());
      deletedTableDesc.set(tableDesc);
    }
  }
  
  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException {
    HTableDescriptor tableDesc = deletedTableDesc.get();
    if (tableDesc != null) {
      HBaseAdmin admin = new HBaseAdmin(ctx.getEnvironment().getConfiguration());
      try {
        for (HColumnDescriptor familyDesc : tableDesc.getColumnFamilies()) {
          if (isSecondaryIndexEnableFamily(familyDesc)) {
            List<String> indexTableNames = getSecondaryIndexTableNames(tableDesc.getNameAsString(),
              familyDesc);
            for (String indexTableName : indexTableNames) {
              admin.disableTable(indexTableName);
              LOG.info("disabled index table name : " + indexTableName);
              admin.deleteTable(indexTableName);
              LOG.info("deleted index table name : " + indexTableName);
            }
          }
        }
      } finally {
        admin.close();
      }
    }
  }
}