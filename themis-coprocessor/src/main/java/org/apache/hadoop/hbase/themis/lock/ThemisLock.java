package org.apache.hadoop.hbase.themis.lock;

import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class ThemisLock implements Writable {
  protected Type type = Type.Minimum; // illegal type should be Type.Put or Type.DeleteColumn
  protected long timestamp;
  protected String clientAddress;
  protected long wallTime;
  protected ColumnCoordinate columnCoordinate; // need not to be serialized
  
  public ColumnCoordinate getColumn() {
    return columnCoordinate;
  }

  public void setColumn(ColumnCoordinate columnCoordinate) {
    this.columnCoordinate = columnCoordinate;
  }

  protected ThemisLock() {}
  
  public ThemisLock(Type type) {
    this.type = type;
  }
  
  public long getTimestamp() {
    return timestamp;
  }
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  
  public abstract boolean isPrimary();
  
  public String getClientAddress() {
    return clientAddress;
  }
  
  public void setClientAddress(String clientAddress) {
    this.clientAddress = clientAddress;
  }

  public long getWallTime() {
    return wallTime;
  }
  
  public void setWallTime(long wallTime) {
    this.wallTime = wallTime;
  }
  
  public Type getType() {
    return this.type;
  }
  
  public void setType(Type type) {
    this.type = type;
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeByte(type.getCode());
    out.writeLong(timestamp);
    Bytes.writeByteArray(out, Bytes.toBytes(clientAddress));
    out.writeLong(wallTime);
  }

  public void readFields(DataInput in) throws IOException {
    this.type = Type.codeToType(in.readByte());
    this.timestamp = in.readLong();
    this.clientAddress = Bytes.toString(Bytes.readByteArray(in));
    this.wallTime = in.readLong();
  }
  
  @Override
  public boolean equals(Object object) {
    if (!(object instanceof ThemisLock)) {
      return false;
    }
    ThemisLock lock = (ThemisLock)object;
    return this.type == lock.type && this.timestamp == lock.timestamp
        && this.wallTime == lock.wallTime && this.clientAddress.equals(lock.clientAddress);
  }
  
  @Override
  public String toString() {
    return "type=" + this.type + "/timestamp=" + this.timestamp + "/wallTime=" + this.wallTime
        + "/clientAddress=" + this.clientAddress + "/column=" + this.columnCoordinate;
  }
  
  public static byte[] toByte(ThemisLock lock) throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutputStream os = new DataOutputStream(byteOut);
    os.writeBoolean(lock.isPrimary());
    lock.write(os);
    return byteOut.toByteArray();
  }
  
  public static ThemisLock parseFromByte(byte[] data) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    boolean isPrimary = in.readBoolean();
    ThemisLock lock = null;
    if (isPrimary) {
      lock = new PrimaryLock();
      lock.readFields(in);
    } else {
      lock = new SecondaryLock();
      lock.readFields(in);
    }
    return lock;
  }
  
  public static void copyThemisLock(ThemisLock source, ThemisLock dest) {
    dest.setTimestamp(source.getTimestamp());
    dest.setWallTime(source.getWallTime());
    dest.setClientAddress(source.getClientAddress());
  }
}