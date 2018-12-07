package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.themis.ThemisScan;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.cache.ColumnMutationCache;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.index.Indexer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class DefaultIndexer extends Indexer {
  private final Map<IndexColumn, TableName> columnIndexes = new HashMap<>();

  public DefaultIndexer(Configuration conf) throws IOException {
    super(conf);
    // could only load secondary indexes created before this construction
    // TODO : add/remove secondary index dynamically when setting/unsetting secondary attribute for
    // table
    loadSecondaryIndexes();
  }

  public Map<IndexColumn, TableName> getColumnIndexes() {
    return columnIndexes;
  }

  protected void loadSecondaryIndexes() throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(getConf());
      Admin admin = conn.getAdmin()) {
      for (TableDescriptor desc : admin.listTableDescriptors()) {
        loadSecondaryIndexesForTable(desc, columnIndexes);
      }
    }
  }

  protected void loadSecondaryIndexesForTable(TableDescriptor desc,
      Map<IndexColumn, TableName> columnIndexes) throws IOException {
    for (ColumnFamilyDescriptor family : desc.getColumnFamilies()) {
      if (IndexMasterObserver.isSecondaryIndexEnableFamily(family)) {
        List<Pair<String, String>> indexNameAndColumns =
          IndexMasterObserver.getIndexNameAndColumns(desc.getTableName(), family);
        for (Pair<String, String> indexNameAndColumn : indexNameAndColumns) {
          String indexName = indexNameAndColumn.getFirst();
          String column = indexNameAndColumn.getSecond();
          IndexColumn indexColumn =
            new IndexColumn(desc.getTableName(), family.getName(), Bytes.toBytes(column));
          TableName indexTableName = IndexMasterObserver.constructSecondaryIndexTableName(
            desc.getTableName(), family.getNameAsString(), column, indexName);
          if (!columnIndexes.containsKey(indexColumn)) {
            columnIndexes.put(indexColumn, indexTableName);
          } else {
            throw new IOException("duplicated index definition found, indexColumn=" + indexColumn);
          }
        }
      }
    }
  }

  @Override
  public IndexScanner getScanner(TableName tableName, ThemisScan scan, Transaction transaction)
      throws IOException {
    if (!(scan instanceof IndexRead)) {
      return null;
    }
    IndexRead indexRead = (IndexRead) scan;
    if (!tableName.equals(indexRead.getIndexColumn().getTableName())) {
      throw new IOException("tableName not match, tableName=" + tableName + ", indexColumn=" +
        indexRead.getIndexColumn());
    }
    TableName indexTableName = columnIndexes.get(indexRead.getIndexColumn());
    if (indexTableName == null) {
      throw new IOException(
        "not find index definition for indexColumn=" + indexRead.getIndexColumn());
    }
    return new IndexScanner(indexTableName, indexRead, transaction);
  }

  @Override
  public void addIndexMutations(ColumnMutationCache mutationCache) throws IOException {
    for (Entry<TableName, Map<byte[], RowMutation>> tableMutation : mutationCache.getMutations()) {
      TableName tableName = tableMutation.getKey();
      for (Entry<byte[], RowMutation> rowMutation : tableMutation.getValue().entrySet()) {
        byte[] row = rowMutation.getKey();
        for (ColumnMutation columnMuation : rowMutation.getValue().mutationList()) {
          if (columnMuation.getType().equals(Type.Put)) {
            IndexColumn indexColumn =
              new IndexColumn(tableName, columnMuation.getFamily(), columnMuation.getQualifier());
            if (columnIndexes.containsKey(indexColumn)) {
              TableName indexTableName = columnIndexes.get(indexColumn);
              Cell indexKv = constructIndexKv(row, columnMuation.getValue());
              mutationCache.addMutation(indexTableName, indexKv);
            }
          }
        }
      }
    }
  }

  protected static Cell constructIndexKv(byte[] mainRowkey, byte[] mainValue) {

    return new KeyValue(mainValue, IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES,
      mainRowkey, Long.MAX_VALUE, Type.Put, HConstants.EMPTY_BYTE_ARRAY);
  }
}
