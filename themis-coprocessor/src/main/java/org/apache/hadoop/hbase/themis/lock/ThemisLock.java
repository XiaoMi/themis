package org.apache.hadoop.hbase.themis.lock;

import org.apache.hadoop.hbase.Cell;
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
  protected Cell.Type type = Cell.Type.DeleteFamilyVersion ; // illegal type should be Type.Put or Type.DeleteColumn
  protected long timestamp;
  protected String clientAddress;
  protected long wallTime; // TODO : remove this field
  protected ColumnCoordinate columnCoordinate; // need not to be serialized
  protected boolean lockExpired = false;
  
  public boolean isLockExpired() {
    return lockExpired;
  }

  public void setLockExpired(boolean lockExpired) {
    this.lockExpired = lockExpired;
  }

  public ColumnCoordinate getColumn() {
    return columnCoordinate;
  }

  public void setColumn(ColumnCoordinate columnCoordinate) {
    this.columnCoordinate = columnCoordinate;
  }

  protected ThemisLock() {}
  
  public ThemisLock(Cell.Type type) {
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

  public long getWallTime() throws IOException {
    throw new IOException("not supportted");
  }
  
  public void setWallTime(long wallTime) throws IOException {
    throw new IOException("not supportted");
  }
  
  public Cell.Type getType() {
    return this.type;
  }
  
  public void setType(Cell.Type type) {
    this.type = type;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(type.name());
    out.writeLong(timestamp);
    Bytes.writeByteArray(out, Bytes.toBytes(clientAddress));
    out.writeLong(wallTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.type = Cell.Type.valueOf(in.readUTF());
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
    ThemisLock lock;
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
    dest.setClientAddress(source.getClientAddress());
  }
}