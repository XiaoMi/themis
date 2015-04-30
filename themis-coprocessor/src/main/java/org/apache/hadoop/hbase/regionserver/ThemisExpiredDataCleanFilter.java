package org.apache.hadoop.hbase.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class ThemisExpiredDataCleanFilter extends FilterBase {
  private static final Log LOG = LogFactory.getLog(ThemisExpiredDataCleanFilter.class);
  private byte[] lastRow;
  private byte[] lastFamily;
  private byte[] lastQualifer;
  private long cleanTs;
  private final HRegion region;
  
  public ThemisExpiredDataCleanFilter() {
    this.region = null;
  }
  
  public ThemisExpiredDataCleanFilter(long cleanTs) {
    this(cleanTs, null);
  }
  
  public ThemisExpiredDataCleanFilter(long cleanTs, HRegion region) {
    this.cleanTs = cleanTs;
    this.region = region;
  }
  
  @Override
  public ReturnCode filterKeyValue(Cell kv) {
    if (kv.getTimestamp() < cleanTs) {
      if (Bytes.equals(lastRow, kv.getRow()) && Bytes.equals(lastFamily, kv.getFamily())
          && Bytes.equals(lastQualifer, kv.getQualifier())) {
        LOG.debug("ExpiredDataCleanFilter, skipColumn kv=" + kv + ", cleanTs=" + cleanTs);
        return ReturnCode.NEXT_COL;
      } else if (this.region != null
          && ColumnUtil.isDeleteColumn(kv.getFamily(), kv.getQualifier())) {
        LOG.debug("ExpiredDataCleanFilter, fist expired kv, kv=" + kv + ", cleanTs=" + cleanTs);

        Delete delete = new Delete(kv.getRow());
        delete.deleteColumns(kv.getFamily(), kv.getQualifier(), kv.getTimestamp());
        Column dataColumn = ColumnUtil.getDataColumn(new Column(kv.getFamily(), kv.getQualifier()));
        delete.deleteColumns(dataColumn.getFamily(), dataColumn.getQualifier(), kv.getTimestamp());
        Column putColumn = ColumnUtil.getPutColumn(dataColumn);
        delete.deleteColumns(putColumn.getFamily(), putColumn.getQualifier(), kv.getTimestamp());
        delete.setWriteToWAL(false);
        try {
          region.delete(delete);
        } catch (IOException e) {
          LOG.error("ExpiredDataCleanFilter delete expired data fail", e);
        }
      }
      lastRow = kv.getRow();
      lastFamily = kv.getFamily();
      lastQualifer = kv.getQualifier();
    }
    return ReturnCode.INCLUDE;
  }
  
  public void readFields(DataInput in) throws IOException {
    this.cleanTs = in.readLong();
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(cleanTs);
  }
}
