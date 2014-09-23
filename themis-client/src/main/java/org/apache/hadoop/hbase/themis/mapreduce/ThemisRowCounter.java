package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// RowCounter in hbase package is not easy to inherit, we copy its copy to do ThemisRowCounter
public class ThemisRowCounter {
  static final String NAME = "ThemisRowCounter";

  static class RowCounterMapper
  extends TableMapper<ImmutableBytesWritable, Result> {

    public static enum Counters {ROWS}

    @Override
    public void map(ImmutableBytesWritable row, Result values,
      Context context)
    throws IOException {
      // Count every row containing data, whether it's in qualifiers or values
      context.getCounter(Counters.ROWS).increment(1);
    }
  }

  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    String tableName = args[0];
    String startKey = null;
    String endKey = null;
    StringBuilder sb = new StringBuilder();

    final String rangeSwitch = "--range=";

    // First argument is table name, starting from second
    for (int i = 1; i < args.length; i++) {
      if (args[i].startsWith(rangeSwitch)) {
        String[] startEnd = args[i].substring(rangeSwitch.length()).split(",", 2);
        if (startEnd.length != 2 || startEnd[1].contains(",")) {
          printUsage("Please specify range in such format as \"--range=a,b\" " +
              "or, with only one boundary, \"--range=,b\" or \"--range=a,\"");
          return null;
        }
        startKey = startEnd[0];
        endKey = startEnd[1];
      }
      else {
        // if no switch, assume column names
        sb.append(args[i]);
        sb.append(" ");
      }
    }

    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(RowCounter.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    if (startKey != null && !startKey.equals("")) {
      scan.setStartRow(Bytes.toBytes(startKey));
    }
    if (endKey != null && !endKey.equals("")) {
      scan.setStopRow(Bytes.toBytes(endKey));
    }
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
    job.setOutputFormatClass(NullOutputFormat.class);
    ThemisTableMapReduceUtil.initTableMapperJob(tableName, scan,
      RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
    job.setNumReduceTasks(0);
    return job;
  }

  private static void printUsage(String errorMessage) {
    System.err.println("ERROR: " + errorMessage);
    printUsage();
  }

  private static void printUsage() {
    System.err.println("Usage: RowCounter [options] <tablename> " +
        "[--range=[startKey],[endKey]] [<column1> <column2>...]");
    System.err.println("For performance consider the following options:\n"
        + "-Dhbase.client.scanner.caching=100\n"
        + "-Dmapred.map.tasks.speculative.execution=false");
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 1) {
      printUsage("Wrong number of parameters: " + args.length);
      System.exit(-1);
    }
    Job job = createSubmittableJob(conf, otherArgs);
    if (job == null) {
      System.exit(-1);
    }
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}