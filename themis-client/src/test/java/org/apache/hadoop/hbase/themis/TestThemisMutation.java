package org.apache.hadoop.hbase.themis;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil.CommitFamily;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestThemisMutation extends TestBase {

  @Test
  public void testCheckContainingPreservedColumns() {
    useCommitFamily(CommitFamily.SAME_WITH_DATA_FAMILY);
    Cell kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(QUALIFIER).setType(Type.Put).build();
    Map<byte[], List<Cell>> mutations = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    mutations.put(FAMILY, new ArrayList<>());
    mutations.get(FAMILY).add(kv);
    try {
      ThemisMutation.checkContainingPreservedColumns(mutations);
    } catch (IOException e) {
      fail();
    }

    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(ColumnUtil.getPutColumn(new Column(FAMILY, QUALIFIER)).getQualifier())
      .setType(Type.Put).build();
    mutations.get(FAMILY).add(kv);
    try {
      ThemisMutation.checkContainingPreservedColumns(mutations);
      fail();
    } catch (IOException e) {
    }
  }

  @Test
  public void testCheckContainingPreservedColumnsForCommitToDifferentFamily() {
    useCommitFamily(CommitFamily.DIFFERNT_FAMILY);
    Cell kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(QUALIFIER).setType(Type.Put).build();
    Map<byte[], List<Cell>> mutations = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    mutations.put(FAMILY, new ArrayList<>());
    mutations.get(FAMILY).add(kv);
    try {
      ThemisMutation.checkContainingPreservedColumns(mutations);
    } catch (IOException e) {
      fail();
    }

    Column putColumn = ColumnUtil.getPutColumn(new Column(FAMILY, QUALIFIER));
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW)
      .setFamily(putColumn.getFamily()).setQualifier(QUALIFIER).setType(Type.Put).build();
    mutations.put(putColumn.getFamily(), new ArrayList<>());
    mutations.get(putColumn.getFamily()).add(kv);
    try {
      ThemisMutation.checkContainingPreservedColumns(mutations);
      fail();
    } catch (IOException e) {
    }
  }
}
