package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.themis.ThemisScan;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.cache.ColumnMutationCache;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.index.Indexer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class DefaultIndexer extends Indexer {
  private final Map<IndexColumn, String> columnIndexes = new HashMap<IndexColumn, String>();

  public DefaultIndexer(Configuration conf) throws IOException {
    super(conf);
    loadSecondaryIndexes();
  }
  
  public Map<IndexColumn, String> getColumnIndexes() {
    return columnIndexes;
  }
  
  protected void loadSecondaryIndexes() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(getConf());
    try {
      HTableDescriptor[] descs = admin.listTables();
      for (HTableDescriptor desc : descs) {
        loadSecondaryIndexesForTable(desc, columnIndexes);
      }
    } finally {
      admin.close();
    }
  }
  
  protected void loadSecondaryIndexesForTable(HTableDescriptor desc,
      Map<IndexColumn, String> columnIndexes) throws IOException {
    for (HColumnDescriptor family : desc.getFamilies()) {
      if (IndexMasterObserver.isSecondaryIndexEnableFamily(family)) {
        List<Pair<String, String>> indexNameAndColumns = IndexMasterObserver
            .getIndexNameAndColumns(desc.getNameAsString(), family);
        for (Pair<String, String> indexNameAndColumn : indexNameAndColumns) {
          String indexName = indexNameAndColumn.getFirst();
          String column = indexNameAndColumn.getSecond();
          IndexColumn indexColumn = new IndexColumn(desc.getName(), family.getName(),
              Bytes.toBytes(column));
          String indexTableName = IndexMasterObserver.constructSecondaryIndexTableName(
            desc.getNameAsString(), family.getNameAsString(), column, indexName);
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
  public IndexScanner getScanner(byte[] tableName, ThemisScan scan, Transaction transaction)
      throws IOException {
    if (!(scan instanceof IndexRead)) {
      return null;
    }
    IndexRead indexRead = (IndexRead)scan;
    if (!Bytes.equals(tableName, indexRead.getIndexColumn().getTableName())) {
      throw new IOException("tableName not match, tableName=" + Bytes.toString(tableName)
          + ", indexColumn=" + indexRead.getIndexColumn());
    }
    String indexTableName = columnIndexes.get(indexRead.getIndexColumn());
    if (indexTableName == null) {
      throw new IOException("not find index definition for indexColumn=" + indexRead.getIndexColumn());
    }
    return new IndexScanner(indexTableName, indexRead, transaction);
  }

  @Override
  public void addIndexMutations(ColumnMutationCache mutationCache) throws IOException {
    for (Entry<byte[], Map<byte[], RowMutation>> tableMutation : mutationCache.getMutations()) {
      byte[] tableName = tableMutation.getKey();
      for (Entry<byte[], RowMutation> rowMutation : tableMutation.getValue().entrySet()) {
        byte[] row = rowMutation.getKey();
        for (ColumnMutation columnMuation : rowMutation.getValue().mutationList()) {
          if (columnMuation.getType().equals(Type.Put)) {
            IndexColumn indexColumn = new IndexColumn(tableName, columnMuation.getFamily(),
                columnMuation.getQualifier());
            if (columnIndexes.containsKey(indexColumn)) {
              String indexTableName = columnIndexes.get(indexColumn);
              KeyValue indexKv = constructIndexKv(row, columnMuation.getValue());
              mutationCache.addMutation(Bytes.toBytes(indexTableName), indexKv);
            }
          }
        }
      }
    }
  }
  
  protected static KeyValue constructIndexKv(byte[] mainRowkey, byte[] mainValue) {
    return new KeyValue(mainValue, IndexMasterObserver.THEMIS_SECONDARY_INDEX_TABLE_FAMILY_BYTES,
        mainRowkey, Long.MAX_VALUE, Type.Put, HConstants.EMPTY_BYTE_ARRAY);
  }
}
