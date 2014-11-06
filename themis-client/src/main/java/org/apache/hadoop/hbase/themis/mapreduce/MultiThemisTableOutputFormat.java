package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.hbase.themis.ThemisDelete;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MultiThemisTableOutputFormat extends OutputFormat<ImmutableBytesWritable, MultiTableMutations> {
  protected static class MultiThemisTableRecordWriter extends
      ThemisTableRecordWriterBase<ImmutableBytesWritable, MultiTableMutations> {

    public MultiThemisTableRecordWriter(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public void doWrite(ImmutableBytesWritable k, MultiTableMutations mutations, Transaction transaction)
        throws IOException, InterruptedException {
      for (TableMutations tableMutation : mutations.mutations) {
        for (Mutation mutation : tableMutation.getMutations()) {
          if (mutation instanceof Put) {
            transaction.put(tableMutation.getTableName(), new ThemisPut((Put) mutation));
          } else if (mutation instanceof Delete) {
            transaction.delete(tableMutation.getTableName(), new ThemisDelete((Delete) mutation));
          } else {
            throw new IOException("action must be put or delete, but is:" + mutation);
          }
        }
      }
    }
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
    // we can't know ahead of time if it's going to blow up when the user
    // passes a table name that doesn't exist, so nothing useful here.
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new TableOutputCommitter();
  }

  @Override
  public RecordWriter<ImmutableBytesWritable, MultiTableMutations> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new MultiThemisTableRecordWriter(HBaseConfiguration.create(context.getConfiguration()));
  }

}
