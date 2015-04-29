package org.apache.hadoop.hbase.themis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil.CommitFamily;
import org.junit.Assert;
import org.junit.Test;

public class TestThemisMutation extends TestBase {
  @Test
  public void testCheckContainingPreservedColumns() {
    useCommitFamily(CommitFamily.SAME_WITH_DATA_FAMILY);
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
  
  @Test
  public void testCheckContainingPreservedColumnsForCommitToDifferentFamily() {
    useCommitFamily(CommitFamily.DIFFERNT_FAMILY);
    KeyValue kv = new KeyValue(ROW, FAMILY, QUALIFIER);
    Map<byte[], List<KeyValue>> mutations = new HashMap<byte[], List<KeyValue>>();
    mutations.put(FAMILY, new ArrayList<KeyValue>());
    mutations.get(FAMILY).add(kv);
    try {
      ThemisMutation.checkContainingPreservedColumns(mutations);
    } catch (IOException e) {
      Assert.fail();
    }
    
    Column putColumn = ColumnUtil.getPutColumn(new Column(FAMILY, QUALIFIER));
    kv = new KeyValue(ROW, putColumn.getFamily(), QUALIFIER);
    mutations.put(putColumn.getFamily(), new ArrayList<KeyValue>());
    mutations.get(putColumn.getFamily()).add(kv);
    try {
      ThemisMutation.checkContainingPreservedColumns(mutations);
      Assert.fail();
    } catch (IOException e) {
    }
  }
}
