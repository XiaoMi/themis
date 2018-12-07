package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ThemisTableRecordReader extends RecordReader<ImmutableBytesWritable, Result> {
  private ThemisTableRecordReaderImpl impl = new ThemisTableRecordReaderImpl();
  
  public void restart(byte[] firstRow) throws IOException {
    this.impl.restart(firstRow);
  }


  public void setScan(Scan scan) {
    this.impl.setScan(scan);
  }

  @Override
  public void close() {
    this.impl.close();
  }

  @Override
  public ImmutableBytesWritable getCurrentKey() throws IOException,
      InterruptedException {
    return this.impl.getCurrentKey();
  }

  @Override
  public Result getCurrentValue() throws IOException, InterruptedException {
    return this.impl.getCurrentValue();
  }

  @Override
  public void initialize(InputSplit inputsplit,
      TaskAttemptContext context) throws IOException,
      InterruptedException {
    this.impl.initialize(inputsplit, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return this.impl.nextKeyValue();
  }

  @Override
  public float getProgress() {
    return this.impl.getProgress();
  }
  
  public void setConf(Configuration conf) {
    this.impl.setConf(conf);
  }
  
  public void setTableName(TableName tableName) {
    this.impl.setTableName(tableName);
  }
}
