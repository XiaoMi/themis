package org.apache.hadoop.hbase.themis.lock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.util.Bytes;

public class PrimaryLock extends ThemisLock {
  static class ColumnCoordinateComparator implements Comparator<ColumnCoordinate> {
    public int compare(ColumnCoordinate o1, ColumnCoordinate o2) {
      int ret = o1.getTableName().compareTo(o2.getTableName());
      if (ret == 0) {
        ret = Bytes.compareTo(o1.getRow(), o2.getRow());
        if (ret == 0) {
          return o1.compareTo(o2);
        }
        return ret;
      }
      return ret;
    }
  }

  private static final ColumnCoordinateComparator COLUMN_COORDINATE_COMPARATOR =
    new ColumnCoordinateComparator();
  protected Map<ColumnCoordinate, Type> secondaryColumns =
    new TreeMap<ColumnCoordinate, Type>(COLUMN_COORDINATE_COMPARATOR);

  public PrimaryLock() {
  }

  public PrimaryLock(Type type) {
    super(type);
  }

  public Type getSecondaryColumn(ColumnCoordinate columnCoordinate) {
    return secondaryColumns.get(columnCoordinate);
  }

  public Map<ColumnCoordinate, Type> getSecondaryColumns() {
    return secondaryColumns;
  }

  public void addSecondaryColumn(ColumnCoordinate columnCoordinate, Type put) {
    this.secondaryColumns.put(columnCoordinate, put);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(secondaryColumns.size());
    for (Entry<ColumnCoordinate, Type> columnAndType : secondaryColumns.entrySet()) {
      columnAndType.getKey().write(out);
      out.writeByte(columnAndType.getValue().getCode());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int secondarySize = in.readInt();
    secondaryColumns = new TreeMap<ColumnCoordinate, Type>(COLUMN_COORDINATE_COMPARATOR);
    for (int i = 0; i < secondarySize; ++i) {
      ColumnCoordinate columnCoordinate = new ColumnCoordinate();
      columnCoordinate.readFields(in);
      Type type = codeToType(in.readByte());
      secondaryColumns.put(columnCoordinate, type);
    }
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof PrimaryLock)) {
      return false;
    }
    PrimaryLock lock = (PrimaryLock) object;
    if (!super.equals((ThemisLock) lock)) {
      return false;
    }
    if (!lock.isPrimary()) {
      return false;
    }
    if (this.secondaryColumns.size() != lock.secondaryColumns.size()) {
      return false;
    }
    for (Entry<ColumnCoordinate, Type> columnAndType : secondaryColumns.entrySet()) {
      Type type = lock.secondaryColumns.get(columnAndType.getKey());
      if (type == null || !columnAndType.getValue().equals(type)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    String result = super.toString() + "/secondariesSize=" + secondaryColumns.size() + "\n";
    for (Entry<ColumnCoordinate, Type> columnAndType : secondaryColumns.entrySet()) {
      result += columnAndType.getKey() + " : " + columnAndType.getValue() + "\n";
    }
    return result;
  }

  @Override
  public boolean isPrimary() {
    return true;
  }
}
