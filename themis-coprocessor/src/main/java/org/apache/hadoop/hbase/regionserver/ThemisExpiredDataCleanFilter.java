package org.apache.hadoop.hbase.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class ThemisExpiredDataCleanFilter extends FilterBase {
  private static final Log LOG = LogFactory.getLog(ThemisExpiredDataCleanFilter.class);
  private byte[] lastRow;
  private byte[] lastFamily;
  private byte[] lastQualifer;
  private long cleanTs;
  
  public ThemisExpiredDataCleanFilter() {
  }
  
  public ThemisExpiredDataCleanFilter(long cleanTs) {
    this.cleanTs = cleanTs;
  }
  
  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    if (kv.getTimestamp() < cleanTs) {
      if (Bytes.equals(lastRow, kv.getRow()) && Bytes.equals(lastFamily, kv.getFamily())
          && Bytes.equals(lastQualifer, kv.getQualifier())) {
        LOG.info("ExpiredDataCleanFilter, skipColumn kv=" + kv + ", cleanTs=" + cleanTs);
        return ReturnCode.NEXT_COL;
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