package org.apache.hadoop.hbase.themis.columns;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.protobuf.HBaseZeroCopyByteString;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.util.Bytes;

// the column with type and value as mutation
public class ColumnMutation extends Column {
  protected Cell.Type type;
  protected byte[] value;
  
  public ColumnMutation() {}
  
  public ColumnMutation(Column column, Cell.Type type, byte[] value) {
    super(column);
    this.type = type;
    this.value = value;
  }
  
  public Cell.Type getType(){
    return type;
  }

  public void setType(Cell.Type type) {
    this.type = type;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    //is this compatible to previous version ?
    out.writeUTF(type.name());
    Bytes.writeByteArray(out, value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    this.type = Cell.Type.valueOf(in.readUTF());
    this.value = Bytes.readByteArray(in);
  }
  
  @Override
  public String toString() {
    return "column=" + super.toString() + ",\type=" + type;
  }

    public static CellProtos.Cell toCell(ColumnMutation mutation) {
        CellProtos.Cell.Builder builder = CellProtos.Cell.newBuilder();
        builder.setFamily(HBaseZeroCopyByteString.wrap(mutation.getFamily()));
        builder.setQualifier(HBaseZeroCopyByteString.wrap(mutation.getQualifier()));
        CellProtos.CellType type = mutation.getType() == Cell.Type.Put ? CellProtos.CellType.PUT : CellProtos.CellType.DELETE_COLUMN;
        builder.setCellType(type);
        if (mutation.getValue() == null) {
            builder.setValue(HBaseZeroCopyByteString.wrap(HConstants.EMPTY_BYTE_ARRAY));
        } else {
            builder.setValue(HBaseZeroCopyByteString.wrap(mutation.getValue()));
        }
        return builder.build();
    }
}
