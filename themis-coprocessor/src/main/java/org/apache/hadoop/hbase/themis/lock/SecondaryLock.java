package org.apache.hadoop.hbase.themis.lock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;

public class SecondaryLock extends ThemisLock {
  protected ColumnCoordinate primaryColumn;

  public SecondaryLock() {
  }

  public SecondaryLock(Type type) {
    super(type);
  }

  public ColumnCoordinate getPrimaryColumn() {
    return primaryColumn;
  }

  public void setPrimaryColumn(ColumnCoordinate primaryColumn) {
    this.primaryColumn = primaryColumn;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    primaryColumn.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    primaryColumn = new ColumnCoordinate();
    primaryColumn.readFields(in);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SecondaryLock)) {
      return false;
    }
    ThemisLock lock = (ThemisLock) other;
    return super.equals(lock) && !lock.isPrimary() &&
      this.primaryColumn.equals(((SecondaryLock) other).getPrimaryColumn());
  }

  @Override
  public String toString() {
    return super.toString() + "/primaryColumn=" + primaryColumn.toString();
  }

  @Override
  public boolean isPrimary() {
    return false;
  }
}