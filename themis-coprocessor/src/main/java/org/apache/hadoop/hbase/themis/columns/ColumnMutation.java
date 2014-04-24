package org.apache.hadoop.hbase.themis.columns;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

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
}
