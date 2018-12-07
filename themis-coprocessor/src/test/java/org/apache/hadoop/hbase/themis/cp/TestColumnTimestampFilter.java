package org.apache.hadoop.hbase.themis.cp;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestColumnTimestampFilter extends TestBase {

  @Test
  public void testFilterKeyValue() {
    ColumnTimestampFilter filter = new ColumnTimestampFilter();
    Cell kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(QUALIFIER).setTimestamp(PREWRITE_TS).setType(Type.Put).setValue(VALUE).build();
    assertEquals(ReturnCode.NEXT_ROW, filter.filterKeyValue(kv));

    filter = new ColumnTimestampFilter();
    filter.addColumnTimestamp(COLUMN, PREWRITE_TS);
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW)
      .setFamily(ANOTHER_FAMILY).setQualifier(QUALIFIER).setTimestamp(PREWRITE_TS).setType(Type.Put)
      .setValue(VALUE).build();
    assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, filter.filterKeyValue(kv));
    assertArrayEquals(CellUtil.cloneFamily(filter.getNextCellHint(kv)), FAMILY);
    assertArrayEquals(CellUtil.cloneQualifier(filter.getNextCellHint(kv)), QUALIFIER);

    filter = new ColumnTimestampFilter();
    filter.addColumnTimestamp(COLUMN, PREWRITE_TS);
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(ANOTHER_QUALIFIER).setTimestamp(PREWRITE_TS).setType(Type.Put).setValue(VALUE)
      .build();
    assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, filter.filterKeyValue(kv));
    assertArrayEquals(CellUtil.cloneFamily(filter.getNextCellHint(kv)), FAMILY);
    assertArrayEquals(CellUtil.cloneQualifier(filter.getNextCellHint(kv)), QUALIFIER);

    filter = new ColumnTimestampFilter();
    filter.addColumnTimestamp(COLUMN, PREWRITE_TS);
    byte[] laterFamily = Bytes.toBytes(Bytes.toString(FAMILY) + "#");
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(laterFamily)
      .setQualifier(QUALIFIER).setTimestamp(PREWRITE_TS).setType(Type.Put).setValue(VALUE).build();
    assertEquals(ReturnCode.NEXT_ROW, filter.filterKeyValue(kv));

    filter = new ColumnTimestampFilter();
    filter.addColumnTimestamp(COLUMN, PREWRITE_TS);
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(QUALIFIER).setTimestamp(PREWRITE_TS - 1).setType(Type.Put).setValue(VALUE)
      .build();
    assertEquals(ReturnCode.NEXT_COL, filter.filterKeyValue(kv));

    filter = new ColumnTimestampFilter();
    filter.addColumnTimestamp(COLUMN, PREWRITE_TS);
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(QUALIFIER).setTimestamp(PREWRITE_TS).setType(Type.Put).setValue(VALUE).build();
    assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL, filter.filterKeyValue(kv));

    filter = new ColumnTimestampFilter();
    filter.addColumnTimestamp(COLUMN, PREWRITE_TS);
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(QUALIFIER).setTimestamp(PREWRITE_TS + 1).setType(Type.Put).setValue(VALUE)
      .build();
    assertEquals(ReturnCode.SKIP, filter.filterKeyValue(kv));

    filter = new ColumnTimestampFilter();
    filter.addColumnTimestamp(COLUMN, PREWRITE_TS);
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(ANOTHER_QUALIFIER).setTimestamp(PREWRITE_TS).setType(Type.Put).setValue(VALUE)
      .build();
    assertEquals(ReturnCode.SEEK_NEXT_USING_HINT, filter.filterKeyValue(kv));
    assertArrayEquals(CellUtil.cloneFamily(filter.getNextCellHint(kv)), FAMILY);
    assertArrayEquals(CellUtil.cloneQualifier(filter.getNextCellHint(kv)), QUALIFIER);
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(QUALIFIER).setTimestamp(PREWRITE_TS + 1).setType(Type.Put).setValue(VALUE)
      .build();
    assertEquals(ReturnCode.SKIP, filter.filterKeyValue(kv));
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(QUALIFIER).setTimestamp(PREWRITE_TS).setType(Type.Put).setValue(VALUE).build();
    assertEquals(ReturnCode.INCLUDE_AND_NEXT_COL, filter.filterKeyValue(kv));
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(QUALIFIER).setTimestamp(PREWRITE_TS - 1).setType(Type.Put).setValue(VALUE)
      .build();
    assertEquals(ReturnCode.NEXT_ROW, filter.filterKeyValue(kv));
    byte[] laterQualifier = Bytes.toBytes(Bytes.toString(QUALIFIER) + "#");
    kv = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(ROW).setFamily(FAMILY)
      .setQualifier(laterQualifier).setTimestamp(PREWRITE_TS + 1).setType(Type.Put).setValue(VALUE)
      .build();
    assertEquals(ReturnCode.NEXT_ROW, filter.filterKeyValue(kv));

  }
}
