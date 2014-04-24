package org.apache.hadoop.hbase.themis.columns;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

// ColumnCoordinate is defined by tableName, row, family and qualifier which
// will location an HBase KeyValue
public class ColumnCoordinate extends Column {
  protected byte[] tableName;
  protected byte[] row;

  public ColumnCoordinate() {
  }

  public ColumnCoordinate(byte[] family, byte[] qualifier) {
    this(null, family, qualifier);
  }

  public ColumnCoordinate(byte[] row, byte[] family, byte[] qualifier) {
    this(null, row, family, qualifier);
  }

  public ColumnCoordinate(byte[] tableName, byte[] row, byte[] family, byte[] qualifier) {
    super(family, qualifier);
    this.tableName = tableName;
    this.row = row;
  }
  
  public ColumnCoordinate(byte[] tableName, byte[] row, Column column) {
    this(tableName, row, column.getFamily(), column.getQualifier());
  }

  public ColumnCoordinate(ColumnCoordinate columnCoordinate) {
    this(columnCoordinate.getTableName(), columnCoordinate.getRow(), columnCoordinate.getFamily(), columnCoordinate.getQualifier());
  }

  public byte[] getTableName() {
    return this.tableName;
  }

  public byte[] getRow() {
    return this.row;
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, tableName);
    Bytes.writeByteArray(out, row);
    super.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    tableName = Bytes.readByteArray(in);
    row = Bytes.readByteArray(in);
    super.readFields(in);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    if (tableName != null) {
      result = prime * result + Bytes.toString(tableName).hashCode();
    }
    if (row != null) {
      result = prime * result + Bytes.toString(row).hashCode();
    }
    return prime * result + super.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ColumnCoordinate)) {
      return false;
    }
    ColumnCoordinate columnCoordinate = (ColumnCoordinate) other;
    return Bytes.equals(this.tableName, columnCoordinate.getTableName())
        && Bytes.equals(this.row, columnCoordinate.getRow()) && super.equals(columnCoordinate);
  }

  @Override
  public String toString() {
    return "tableName=" + Bytes.toString(tableName) + "/row=" + Bytes.toString(row) + "/"
        + super.toString();
  }
}
