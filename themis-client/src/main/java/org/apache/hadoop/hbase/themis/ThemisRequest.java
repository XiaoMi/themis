package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;

// abstract class for ThemisGet/Delete/Put/Scan
abstract class ThemisRequest {
  // check whether the requested family/qualifier refers to preserved family/qualifier
  protected void checkContainingPreservedColumn(byte[] family, byte[] qualifier) throws IOException {
    Column column = new Column(family, qualifier == null ? HConstants.EMPTY_BYTE_ARRAY : qualifier);
    if (ColumnUtil.isPreservedColumn(column)) {
      throw new IOException("can not query preserved column : " + column);
    }
  }
  
  // check the request must contain at least one column
  public static void checkContainColumn(ThemisRequest request) throws IOException {
    if (!request.hasColumn()) {
      throw new IOException("must set at least one column for themis request class : "
          + request.getClass());
    }
  }
  
  protected abstract boolean hasColumn();
}

abstract class ThemisMutation extends ThemisRequest {
  public abstract Map<byte [], List<KeyValue>> getFamilyMap();
  
  protected boolean hasColumn() {
    return getFamilyMap() != null && getFamilyMap().size() != 0;
  }
}