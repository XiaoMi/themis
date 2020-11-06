package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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
    final TableName tableName = tSplit.getTable();
    Table table = getTable(tableName);

    if (table == null) {
     throw new IOException("Cannot create a record reader because of a"
          + " previous error. Please look at the previous logs lines from"
          + " the task's full log for more details.");
    }

    ThemisTableRecordReader trr = this.themisTableRecordReader;
    if (trr == null) {
      trr = new ThemisTableRecordReader();
    }
    Scan sc = new Scan(this.getScan());
    sc.setStartRow(tSplit.getStartRow());
    sc.setStopRow(tSplit.getEndRow());
    trr.setScan(sc);
    trr.setConf(table.getConfiguration());
    trr.setTableName(tableName.getName());
    try {
      trr.initialize(tSplit, context);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }
    return trr;
  }

  private Table getTable(TableName tableName) throws IOException {
      Configuration configuration = this.getConf();

      try (Connection connection = ConnectionFactory.createConnection(configuration)) {
          return connection.getTable(tableName);
      }
  }
}