package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

public class MultiThemisTableReducer
extends TableReducer<ImmutableBytesWritable, TableMutations, ImmutableBytesWritable> {
  @Override
  public void reduce(ImmutableBytesWritable key, Iterable<TableMutations> values, Context context)
      throws IOException, InterruptedException {
    MultiTableMutations mutations = new MultiTableMutations();
    for (TableMutations value : values) {
      TableMutations mutation = value.cloneTableMuations();
      mutations.add(mutation);
    }
    context.write(key, mutations);    
 }
}