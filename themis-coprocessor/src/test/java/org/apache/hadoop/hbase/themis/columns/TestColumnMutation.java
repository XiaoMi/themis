package org.apache.hadoop.hbase.themis.columns;

import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.CellType;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell.Builder;
import org.apache.hadoop.hbase.themis.TestBase;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.HBaseZeroCopyByteString;

public class TestColumnMutation extends TestBase {
  @Test
  public void testCellToColumnMutation() {
    Builder builder = Cell.newBuilder();
    builder.setFamily(HBaseZeroCopyByteString.wrap(FAMILY));
    builder.setQualifier(HBaseZeroCopyByteString.wrap(QUALIFIER));
    builder.setCellType(CellType.PUT);
    builder.setValue(HBaseZeroCopyByteString.wrap(VALUE));
    ColumnMutation mutation = ColumnMutation.toColumnMutation(builder.build());
    Assert.assertArrayEquals(FAMILY, mutation.getFamily());
    Assert.assertArrayEquals(QUALIFIER, mutation.getQualifier());
    Assert.assertEquals(Type.Put, mutation.getType());
    Assert.assertArrayEquals(VALUE, mutation.getValue());
    
    builder.setCellType(CellType.DELETE);
    mutation = ColumnMutation.toColumnMutation(builder.build());
    Assert.assertArrayEquals(FAMILY, mutation.getFamily());
    Assert.assertArrayEquals(QUALIFIER, mutation.getQualifier());
    Assert.assertEquals(Type.DeleteColumn, mutation.getType());
    Assert.assertArrayEquals(VALUE, mutation.getValue());
  }
  
  @Test
  public void testColumnMutationToCell() {
    ColumnMutation columnMutaiton = new ColumnMutation(new Column(FAMILY, QUALIFIER), Type.Put, VALUE);
    Cell cell = ColumnMutation.toCell(columnMutaiton);
    Assert.assertArrayEquals(FAMILY, cell.getFamily().toByteArray());
    Assert.assertArrayEquals(QUALIFIER, cell.getQualifier().toByteArray());
    Assert.assertEquals(CellType.PUT, cell.getCellType());
    Assert.assertArrayEquals(VALUE, cell.getValue().toByteArray());
    
    columnMutaiton.setType(Type.DeleteColumn);
    cell = ColumnMutation.toCell(columnMutaiton);
    Assert.assertArrayEquals(FAMILY, cell.getFamily().toByteArray());
    Assert.assertArrayEquals(QUALIFIER, cell.getQualifier().toByteArray());
    Assert.assertEquals(CellType.DELETE_COLUMN, cell.getCellType());
    Assert.assertArrayEquals(VALUE, cell.getValue().toByteArray());
  }
}
