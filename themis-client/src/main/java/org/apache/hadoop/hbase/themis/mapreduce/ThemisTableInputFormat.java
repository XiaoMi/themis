package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ThemisTableInputFormat extends TableInputFormat {
  private ThemisTableRecordReader themisTableRecordReader = null;
  
  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    TableSplit tSplit = (TableSplit) split;
    ThemisTableRecordReader trr = this.themisTableRecordReader;
    if (trr == null) {
      trr = new ThemisTableRecordReader();
    }
    Scan sc = new Scan(this.getScan());
    sc.withStartRow(tSplit.getStartRow());
    sc.withStopRow(tSplit.getEndRow());
    trr.setScan(sc);
    trr.setConf(getTable().getConfiguration());
    trr.setTableName(getTable().getName());
    try {
      trr.initialize(tSplit, context);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }
    return trr;
  }
}