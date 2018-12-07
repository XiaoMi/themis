package org.apache.hadoop.hbase.themis.cp;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class ExcludeDataColumnFilter extends FilterBase {

  @Override
  public ReturnCode filterKeyValue(Cell kv) {
    byte[] family = CellUtil.cloneFamily(kv);
    if (!Bytes.equals(ColumnUtil.LOCK_FAMILY_NAME, family) &&
      !ColumnUtil.isWriteColumn(family, CellUtil.cloneQualifier(kv))) {
      return ReturnCode.NEXT_COL;
    }
    return ReturnCode.INCLUDE_AND_NEXT_COL;
  }
}
