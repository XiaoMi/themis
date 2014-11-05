package org.apache.hadoop.hbase.themis.index.cp;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.apache.hadoop.hbase.themis.ThemisScanner;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.util.Bytes;

// TODO : set batch for index scan to avoid super-big row
public class IndexScanner extends ThemisScanner {
  private static final Log LOG = LogFactory.getLog(IndexScanner.class);
  private IndexColumn indexColumn;
  private IndexRead indexRead;
  private Result indexResult = null;
  private int kvIndex = 0;
  private boolean done = false;
  private long unmatchIndexCount = 0;
  
  public IndexScanner(String indexTableName, IndexRead indexRead, Transaction transaction)
      throws IOException {
    super(Bytes.toBytes(indexTableName), indexRead.getIndexScan().getInternalScan(), transaction);
    this.indexRead = indexRead;
    this.indexColumn = indexRead.getIndexColumn();
  }
  
  public Result next() throws IOException {
    if (done) {
      return null;
    }
    
    while (true) {
      if (indexResult == null || (++kvIndex == indexResult.size())) {
        indexResult = super.next();
        if (indexResult == null) {
          done = true;
          return null;
        }
        kvIndex = 0;
      }
      
      KeyValue indexKv = indexResult.list().get(kvIndex);
      ThemisGet dataRowGet = constructDataRowGet(indexKv.getQualifier(), indexRead.dataGet);
      Result dataResult = transaction.get(indexColumn.getTableName(), dataRowGet);
      KeyValue indexColumnKv = dataResult.getColumnLatest(indexColumn.getFamily(),
        indexColumn.getQualifier());
      if (indexColumnKv == null || indexColumnKv.getTimestamp() != indexKv.getTimestamp()) {
        LOG.info("find unmatch index, indexKv=" + indexKv + ", indexColumnKv=" + indexColumnKv
            + ", totalUnMatchIndexCount=" + (++unmatchIndexCount));
        continue;
      }
      return dataResult;
    }
  }
  
  public long getUnMatchIndexCount() {
    return unmatchIndexCount;
  }
  
  protected static ThemisGet constructDataRowGet(byte[] row, ThemisGet dataGet) throws IOException {
    ThemisGet get = new ThemisGet(row);
    for (Entry<byte[], NavigableSet<byte[]>> columns : dataGet.getFamilyMap().entrySet()) {
      byte[] family = columns.getKey();
      if (columns.getValue() != null && columns.getValue().size() > 0) {
        for (byte[] qualifier : columns.getValue()) {
          get.addColumn(family, qualifier);
        }
      } else {
        get.addFamily(family);
      }
    }
    get.setFilter(dataGet.getFilter());
    get.setCacheBlocks(dataGet.getCacheBlocks());
    return get;
  }
}
