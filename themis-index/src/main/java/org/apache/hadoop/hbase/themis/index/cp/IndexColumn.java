package org.apache.hadoop.hbase.themis.index.cp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.util.Bytes;

public class IndexColumn extends Column {
  private TableName tableName;

  public IndexColumn(TableName tableName, byte[] family, byte[] qualifier) {
    super(family, qualifier);
    this.tableName = tableName;
  }

  public IndexColumn(TableName tableName, String family, String qualifier) {
    this(tableName, Bytes.toBytes(family), Bytes.toBytes(qualifier));
  }

  public TableName getTableName() {
    return tableName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    if (tableName != null) {
      result = prime * result + tableName.hashCode();
    }
    return prime * result + super.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IndexColumn)) {
      return false;
    }
    IndexColumn indexColumn = (IndexColumn) other;
    return this.tableName.equals(indexColumn.tableName) && super.equals(indexColumn);
  }

  @Override
  public String toString() {
    return "tableName=" + tableName + "/" + super.toString();
  }

  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }
}
