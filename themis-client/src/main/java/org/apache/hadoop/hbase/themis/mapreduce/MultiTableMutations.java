package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.io.Writable;

public class MultiTableMutations extends Mutation implements Writable {
  
  public List<TableMutations> mutations = new ArrayList<TableMutations>();

  public MultiTableMutations() {
  }

  public void add(TableMutations tableMutation) {
    mutations.add(tableMutation);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(mutations.size());
    for (TableMutations mutation : mutations) {
      mutation.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    mutations.clear();
    int mutationSize = in.readInt();
    for (int i = 0; i < mutationSize; i++) {
      TableMutations tableMutations = new TableMutations();
      tableMutations.readFields(in);
      mutations.add(tableMutations);
    }
  }

  @Override
  public String toString() {
    String result = "table mutation size=" + mutations.size() + "\n";
    for (TableMutations mutation : mutations) {
      result += mutation.toString();
    }
    return result;
  }
}
