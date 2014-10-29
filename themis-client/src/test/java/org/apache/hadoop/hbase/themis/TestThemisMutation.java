package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestThemisMutation extends TestBase {
  @Test
  public void testCheckContainingPreservedColumns() {
    KeyValue kv = new KeyValue(ROW, FAMILY, QUALIFIER);
    Map<byte[], List<KeyValue>> mutations = new HashMap<byte[], List<KeyValue>>();
    mutations.put(FAMILY, new ArrayList<KeyValue>());
    mutations.get(FAMILY).add(kv);
    try {
      ThemisMutation.checkContainingPreservedColumns(mutations);
    } catch (IOException e) {
      Assert.fail();
    }
    
    kv = new KeyValue(ROW, FAMILY, ColumnUtil.getPutColumn(new Column(FAMILY, QUALIFIER))
        .getQualifier());
    mutations.get(FAMILY).add(kv);
    try {
      ThemisMutation.checkContainingPreservedColumns(mutations);
      Assert.fail();
    } catch (IOException e) {
    }
  }
}
