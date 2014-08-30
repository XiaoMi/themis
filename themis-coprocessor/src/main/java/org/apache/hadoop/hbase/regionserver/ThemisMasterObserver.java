package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class ThemisMasterObserver extends BaseMasterObserver {
  public static final String THEMIS_ENABLE_KEY = "THEMIS_ENABLE";
  
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    boolean themisEnable = false;
    for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
      String value = columnDesc.getValue(THEMIS_ENABLE_KEY);
      if (value != null && Boolean.parseBoolean(value)) {
        themisEnable = true;
        break;
      }
    }
    
    if (themisEnable) {
      for (HColumnDescriptor columnDesc : desc.getColumnFamilies()) {
        if (Bytes.equals(ColumnUtil.LOCK_FAMILY_NAME, columnDesc.getName())) {
          throw new IOException(ColumnUtil.LOCK_FAMILY_NAME_STRING
              + " family is preserved by themis when " + THEMIS_ENABLE_KEY
              + " is true, please change your family name");
        }
      }
    }
  }
}
