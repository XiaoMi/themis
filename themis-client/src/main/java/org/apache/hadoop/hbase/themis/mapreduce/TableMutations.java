package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class TableMutations extends Mutation implements Writable {
  private List<Mutation> mutations = new ArrayList<Mutation>();
  private byte [] tableName;

  /** Constructor for Writable. DO NOT USE */
  public TableMutations() {}

  public TableMutations(byte[] tableName) {
    this.tableName = tableName;
  }
  
  public void add(Mutation m) throws IOException {
    mutations.add(m);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    this.tableName = Bytes.readByteArray(in);
    int numMutations = in.readInt();
    mutations.clear();

    for(int i = 0; i < numMutations; i++) {
      readMutation(in);
    }
  }

  private void readMutation(DataInput in) throws IOException {
    ByteArrayInputStream bin = null;
    ObjectInputStream ois = null;
    try {
      int size = in.readInt();
      byte[] data = new byte[size];
      in.readFully(data);

      bin = new ByteArrayInputStream(data);
      ois = new ObjectInputStream(bin);

      mutations.add((Mutation) ois.readObject());
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      if (bin != null) {
        bin.close();
      }

      if (ois != null) {
        ois.close();
      }
    }
  }

    @Override
  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.tableName);
    out.writeInt(mutations.size());
    for (Mutation m : mutations) {
      writeMutation(out, m);
    }
  }

  private void writeMutation(DataOutput out, Mutation m) throws IOException {
    ByteArrayOutputStream bos = null;
    ObjectOutputStream oos = null;
    try {
      bos = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(bos);
      oos.writeObject(m);

      byte[] data = bos.toByteArray();
      int size = data.length;
      out.writeInt(size);
      out.write(data);
    } finally {
     if(bos!=null) {
       bos.close();
     }

     if(oos!=null) {
       oos.close();
     }
    }
  }

  public TableMutations cloneTableMuations() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    this.write(new DataOutputStream(os));
    TableMutations cloned = new TableMutations();
    cloned.readFields(new DataInputStream(new ByteArrayInputStream(os.toByteArray())));
    return cloned;
  }

  public List<Mutation> getMutations() {
    return Collections.unmodifiableList(mutations);
  }
  
  public byte[] getTableName() {
    return tableName;
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder("TableName=" + Bytes.toString(tableName) + "\n");
    for (Mutation mutation : mutations) {
      result.append(mutation.toString()).append("\n");
    }
    return result.toString();
  }
}