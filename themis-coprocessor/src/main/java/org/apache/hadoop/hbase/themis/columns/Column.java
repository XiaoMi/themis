package org.apache.hadoop.hbase.themis.columns;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

// column is represents a family and qualifier
public class Column implements Writable, Comparable<Column> {
  protected byte[] family;
  protected byte[] qualifier;

  public Column() {}

  public Column(byte[] family, byte[] qualifier) {
    this.family = family;
    this.qualifier = qualifier;
  }
  
  public Column(Column column) {
    this.family = column.family;
    this.qualifier = column.qualifier;
  }
  
  public Column(String family, String qualifier) {
    this(Bytes.toBytes(family), Bytes.toBytes(qualifier));
  }
  
  public byte[] getFamily() {
    return this.family;
  }

  public byte[] getQualifier() {
    return this.qualifier;
  }
  
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, family);
    Bytes.writeByteArray(out, qualifier);
  }

  public void readFields(DataInput in) throws IOException {
    family = Bytes.readByteArray(in);
    qualifier = Bytes.readByteArray(in);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    if (family != null) {
      result = prime * result + Bytes.toString(family).hashCode();
    }
    if (qualifier != null) {
      result = prime * result + Bytes.toString(qualifier).hashCode();
    }
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Column)) {
      return false;
    }
    Column column = (Column) other;
    return Bytes.equals(this.family, column.getFamily())
        && Bytes.equals(this.qualifier, column.getQualifier());
  }

  @Override
  public String toString() {
    return "family=" + Bytes.toString(family) + "/qualifier=" + Bytes.toStringBinary(qualifier);
  }

  public int compareTo(Column other) {
    int ret = Bytes.compareTo(this.family, other.family);
    if (ret == 0) {
      return Bytes.compareTo(this.qualifier, other.qualifier);
    }
    return ret;
  }
}
