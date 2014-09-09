package org.apache.hadoop.hbase.themis.columns;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell.Builder;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.CellType;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;

// the column with type and value as mutation
public class ColumnMutation extends Column {
  protected Type type;
  protected byte[] value;
  
  public ColumnMutation() {}
  
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
  
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeByte(type.getCode());
    Bytes.writeByteArray(out, value);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.type = Type.codeToType(in.readByte());
    this.value = Bytes.readByteArray(in);
  }
  
  @Override
  public String toString() {
    return "column=" + super.toString() + ",\type=" + type;
  }
  
  public KeyValue toKeyValue(byte[] row, long timestamp) {
    return new KeyValue(row, family, qualifier, timestamp, type, value);
  }

  // TODO : add unit test
  public static Cell toCell(ColumnMutation mutation) {
    Builder builder = Cell.newBuilder();
    builder.setFamily(HBaseZeroCopyByteString.wrap(mutation.getFamily()));
    builder.setQualifier(HBaseZeroCopyByteString.wrap(mutation.getQualifier()));
    CellType type = mutation.getType() == Type.Put ? CellType.PUT : CellType.DELETE_COLUMN;
    builder.setCellType(type);
    if (mutation.getValue() == null) {
      // TODO : is this the best method?
      builder.setValue(HBaseZeroCopyByteString.wrap(HConstants.EMPTY_BYTE_ARRAY));
    } else {
      builder.setValue(HBaseZeroCopyByteString.wrap(mutation.getValue()));
    }
    return builder.build();
  }
  
  // TODO : add unit test
  public static ColumnMutation toColumnMutation(Cell cell) {
    CellType type = cell.getCellType();
    Type kvType = type == CellType.PUT ? Type.Put : Type.DeleteColumn;
    // is toByteArray correct?
    ColumnMutation mutation = new ColumnMutation(new Column(cell.getFamily().toByteArray(), cell
        .getQualifier().toByteArray()), kvType, cell.getValue().toByteArray());
    return mutation;
  }
  
  // TODO : add unit test
  public static List<ColumnMutation> toColumnMutations(List<Cell> cells) {
    List<ColumnMutation> mutations = new ArrayList<ColumnMutation>(cells.size());
    for (Cell cell : cells) {
      mutations.add(toColumnMutation(cell));
    }
    return mutations;
  }
}
