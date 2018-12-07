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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class TableMutations implements Writable {
  private List<Mutation> mutations = new ArrayList<Mutation>();
  private TableName tableName;

  /** Constructor for Writable. DO NOT USE */
  public TableMutations() {
  }

  public TableMutations(TableName tableName) {
    this.tableName = tableName;
  }

  public void add(Mutation m) throws IOException {
    mutations.add(m);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    this.tableName = TableName.valueOf(Bytes.readByteArray(in));
    int numMutations = in.readInt();
    mutations.clear();
    for (int i = 0; i < numMutations; i++) {
      MutationProto proto = MutationProto.parseDelimitedFrom((DataInputStream) in);
      mutations.add(ProtobufUtil.toMutation(proto));
    }
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.tableName.getName());
    out.writeInt(mutations.size());
    for (Mutation mutation : mutations) {
      MutationType type;
      if (mutation instanceof Put) {
        type = MutationType.PUT;
      } else if (mutation instanceof Delete) {
        type = MutationType.DELETE;
      } else {
        throw new IllegalArgumentException("Only Put and Delete are supported");
      }
      ProtobufUtil.toMutation(type, mutation).writeDelimitedTo((DataOutputStream) out);
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

  public TableName getTableName() {
    return tableName;
  }

  @Override
  public String toString() {
    String result = "TableName=" + tableName + "\n";
    for (Mutation mutation : mutations) {
      result += (mutation.toString() + "\n");
    }
    return result;
  }
}
