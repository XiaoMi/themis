package org.apache.hadoop.hbase.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
      if (Bytes.equals(lastRow, CellUtil.cloneRow(kv)) && Bytes.equals(lastFamily, CellUtil.cloneFamily(kv))
          && Bytes.equals(lastQualifer, CellUtil.cloneQualifier(kv))) {
        LOG.debug("ExpiredDataCleanFilter, skipColumn kv=" + kv + ", cleanTs=" + cleanTs);
        return ReturnCode.NEXT_COL;
      } else if (this.region != null
          && ColumnUtil.isDeleteColumn(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv))) {
        LOG.debug("ExpiredDataCleanFilter, fist expired kv, kv=" + kv + ", cleanTs=" + cleanTs);

        Delete delete = new Delete(CellUtil.cloneRow(kv));
        delete.addColumns(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv), kv.getTimestamp());

        Column dataColumn = ColumnUtil.getDataColumn(new Column(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv)));
        delete.addColumns(dataColumn.getFamily(), dataColumn.getQualifier(), kv.getTimestamp());
        Column putColumn = ColumnUtil.getPutColumn(dataColumn);
        delete.addColumns(putColumn.getFamily(), putColumn.getQualifier(), kv.getTimestamp());
        try {
          region.delete(delete);
        } catch (IOException e) {
          LOG.error("ExpiredDataCleanFilter delete expired data fail", e);
        }
      }
      lastRow = CellUtil.cloneRow(kv);
      lastFamily = CellUtil.cloneFamily(kv);
      lastQualifer = CellUtil.cloneQualifier(kv);
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