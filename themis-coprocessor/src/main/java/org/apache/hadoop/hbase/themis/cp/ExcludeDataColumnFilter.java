package org.apache.hadoop.hbase.themis.cp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class ExcludeDataColumnFilter extends FilterBase {

  @Override
  public ReturnCode filterKeyValue(Cell kv) {
    if (!Bytes.equals(ColumnUtil.LOCK_FAMILY_NAME, kv.getFamily())
        && !ColumnUtil.isWriteColumn(kv.getFamily(), kv.getQualifier())) {
      return ReturnCode.NEXT_COL;
    }
    return ReturnCode.INCLUDE_AND_NEXT_COL;
  }
  
  public void readFields(DataInput arg0) throws IOException {
    throw new IOException("not implemented");
  }

  public void write(DataOutput arg0) throws IOException {
    throw new IOException("not implemented");
  }

}
