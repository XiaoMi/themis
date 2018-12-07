package org.apache.hadoop.hbase.themis.columns;

import com.google.protobuf.HBaseZeroCopyByteString;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell.Builder;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.CellType;

// the column with type and value as mutation
public class ColumnMutation extends Column {
  private Type type;
  private byte[] value;

  public ColumnMutation() {
  }

  public ColumnMutation(Column column, Type type, byte[] value) {
    super(column);
    this.type = type;
    this.value = value;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "column=" + super.toString() + ",\type=" + type;
  }

  public static Cell toCell(ColumnMutation mutation) {
    Builder builder = Cell.newBuilder();
    builder.setFamily(HBaseZeroCopyByteString.wrap(mutation.getFamily()));
    builder.setQualifier(HBaseZeroCopyByteString.wrap(mutation.getQualifier()));
    CellType type = mutation.getType() == Type.Put ? CellType.PUT : CellType.DELETE_COLUMN;
    builder.setCellType(type);
    if (mutation.getValue() == null) {
      builder.setValue(HBaseZeroCopyByteString.wrap(HConstants.EMPTY_BYTE_ARRAY));
    } else {
      builder.setValue(HBaseZeroCopyByteString.wrap(mutation.getValue()));
    }
    return builder.build();
  }

  public static ColumnMutation toColumnMutation(Cell cell) {
    CellType type = cell.getCellType();
    Type kvType = type == CellType.PUT ? Type.Put : Type.DeleteColumn;
    ColumnMutation mutation = new ColumnMutation(
      new Column(cell.getFamily().toByteArray(), cell.getQualifier().toByteArray()), kvType,
      cell.getValue().toByteArray());
    return mutation;
  }

  public static List<ColumnMutation> toColumnMutations(List<Cell> cells) {
    List<ColumnMutation> mutations = new ArrayList<ColumnMutation>(cells.size());
    for (Cell cell : cells) {
      mutations.add(toColumnMutation(cell));
    }
    return mutations;
  }
}
