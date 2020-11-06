package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;

// abstract class for ThemisGet/Delete/Put/Scan
abstract class ThemisRequest {
  // check whether the requested family/qualifier refers to preserved family/qualifier
  protected static void checkContainingPreservedColumn(byte[] family, byte[] qualifier) throws IOException {
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

  /**
   *
   * @return
   */
  protected abstract boolean hasColumn();
}

abstract class ThemisMutation extends ThemisRequest {
  public abstract Map<byte [], List<Cell>> getFamilyMap();

  @Override
  protected boolean hasColumn() {
    return getFamilyMap() != null && getFamilyMap().size() != 0;
  }
  
  public static void checkContainingPreservedColumns(Map<byte[], List<Cell>> mutations)
      throws IOException {
    for (Entry<byte[], List<Cell>> entry : mutations.entrySet()) {
      for (Cell kv : entry.getValue()) {
        checkContainingPreservedColumn(kv.getFamilyArray(), kv.getQualifierArray());
      }
    }
  }
}