package org.apache.hadoop.hbase.themis.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.mapreduce.ThemisRowCounter.RowCounterMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.Test;

public class TestMultiThemisTableInputFormat extends TestThemisRowCounter {
  protected Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    String[] tableNames = args[0].split(":");
    StringBuilder sb = new StringBuilder();

    for (int i = 1; i < args.length; i++) {
      sb.append(args[i]);
      sb.append(" ");
    }

    Job job = Job.getInstance(conf, "MultiThemisTableRowCounter_" + args[0]);
    job.setJarByClass(RowCounter.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    scan.setFilter(new FirstKeyOnlyFilter());
    if (sb.length() > 0) {
      for (String columnName : sb.toString().trim().split(" ")) {
        String family = StringUtils.substringBefore(columnName, ":");
        String qualifier = StringUtils.substringAfter(columnName, ":");
        if (StringUtils.isBlank(qualifier)) {
          throw new IOException("must specify qualifier to read themis table");
        } else {
          scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        }
      }
    }

    List<Scan> scans = new ArrayList<Scan>();
    for (String tableName : tableNames) {
      Scan thisScan = new Scan(scan);
      thisScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName));
      scans.add(thisScan);
    }

    job.setOutputFormatClass(NullOutputFormat.class);
    ThemisTableMapReduceUtil.initTableMapperJob(scans, RowCounterMapper.class,
      ImmutableBytesWritable.class, Result.class, job);
    job.setNumReduceTasks(0);
    return job;
  }

  protected void writeTestData() throws IOException {
    Transaction transaction = new Transaction(connection);
    for (TableName tableName : new TableName[] { TABLENAME, ANOTHER_TABLENAME }) {
      transaction.put(tableName, new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE)
        .add(ANOTHER_FAMILY, QUALIFIER, VALUE).add(FAMILY, ANOTHER_FAMILY, VALUE));
      transaction.put(tableName, new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, VALUE));
    }
    transaction.commit();
  }

  @Test
  public void testThemisRowCounter() throws Exception {
    writeTestData();
    Job job = createSubmittableJob(conf, new String[] { TABLENAME.getNameAsString(),
      Bytes.toString(FAMILY) + ":" + Bytes.toString(QUALIFIER) });
    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());
    Counter counter =
      job.getCounters().findCounter(ThemisRowCounter.RowCounterMapper.Counters.ROWS);
    assertEquals(2, counter.getValue());

    job = createSubmittableJob(conf,
      new String[] { TABLENAME.getNameAsString() + ":" + ANOTHER_TABLENAME.getNameAsString(),
        Bytes.toString(FAMILY) + ":" + Bytes.toString(QUALIFIER) });
    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());
    counter = job.getCounters().findCounter(ThemisRowCounter.RowCounterMapper.Counters.ROWS);
    assertEquals(4, counter.getValue());
  }
}
