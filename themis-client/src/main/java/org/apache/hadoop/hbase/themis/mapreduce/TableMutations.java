package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
// import org.apache.hadoop.hbase.io.HbaseObjectWritable; // TODO
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class TableMutations implements Writable {
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
    // TODO : make this compatiable
    //for(int i = 0; i < numMutations; i++) {
    //  mutations.add((Mutation) HbaseObjectWritable.readObject(in, null));
    //}
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.tableName);
    out.writeInt(mutations.size());
    // TODO : make this compatiable
    //for (Mutation m : mutations) {
    //  HbaseObjectWritable.writeObject(out, m, m.getClass(), null);
    //}
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
  
  public String toString() {
    String result = "TableName=" + Bytes.toString(tableName) + "\n";
    for (Mutation mutation : mutations) {
      result += (mutation.toString() + "\n");
    }
    return result;
  }
}
