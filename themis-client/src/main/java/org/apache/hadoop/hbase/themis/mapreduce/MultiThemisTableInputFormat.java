package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MultiThemisTableInputFormat extends MultiTableInputFormat {
  private ThemisTableRecordReader themisTableRecordReader = null;

  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    TableSplit tSplit = (TableSplit) split;

    if (tSplit.getTableName() == null) {
      throw new IOException("Cannot create a record reader because of a" +
        " previous error. Please look at the previous logs lines from" +
        " the task's full log for more details.");
    }
    ThemisTableRecordReader trr = this.themisTableRecordReader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new ThemisTableRecordReader();
    }
    Scan sc = tSplit.getScan();
    sc.withStartRow(tSplit.getStartRow());
    sc.withStopRow(tSplit.getEndRow());
    trr.setScan(sc);
    trr.setConf(context.getConfiguration());
    trr.setTableName(TableName.valueOf(tSplit.getTableName()));
    trr.initialize(split, context);
    return trr;
  }
}
