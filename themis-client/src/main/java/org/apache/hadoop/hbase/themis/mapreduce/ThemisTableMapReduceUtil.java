package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpointClient;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;

public class ThemisTableMapReduceUtil {
  static Log LOG = LogFactory.getLog(TableMapReduceUtil.class);
  
  public static void initTableMapperJob(String table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job)
  throws IOException {
    initTableMapperJob(table, scan, mapper, outputKeyClass, outputValueClass,
        job, true);
  }


   public static void initTableMapperJob(byte[] table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job)
  throws IOException {
      initTableMapperJob(Bytes.toString(table), scan, mapper, outputKeyClass, outputValueClass,
              job, true);
  }

  public static void initTableMapperJob(String table, Scan scan,
      Class<? extends TableMapper> mapper, Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job, boolean addDependencyJars)
      throws IOException {
    initTableMapperJob(table, scan, mapper, outputKeyClass, outputValueClass, job,
      addDependencyJars, ThemisTableInputFormat.class);
  }
   
  public static void initTableMapperJob(String table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job,
      boolean addDependencyJars, Class<? extends InputFormat> inputFormatClass)
  throws IOException {
    job.setInputFormatClass(inputFormatClass);
    if (outputValueClass != null) job.setMapOutputValueClass(outputValueClass);
    if (outputKeyClass != null) job.setMapOutputKeyClass(outputKeyClass);
    job.setMapperClass(mapper);
    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
    conf.setStrings("io.serializations", conf.get("io.serializations"),
      WritableSerialization.class.getName(), ResultSerialization.class.getName());

    conf.set(TableInputFormat.INPUT_TABLE, table);
    conf.set(TableInputFormat.SCAN, convertScanToString(scan));
    if (addDependencyJars) {
      addDependencyJars(job);
    }
    TableMapReduceUtil.initCredentials(job);
  }
  
  // this method is coped from TableMapReduceUtil.java because it is protected
  static String convertScanToString(Scan scan) throws IOException {
    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
    return Base64.encodeBytes(proto.toByteArray());
  }
  
  public static void addDependencyJars(Job job) throws IOException {
    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(), ThemisEndpointClient.class);
  }
  
  public static void initTableMapperJob(List<Scan> scans,
      Class<? extends TableMapper> mapper,
      Class<? extends WritableComparable> outputKeyClass,
      Class<?> outputValueClass, Job job) throws IOException {
    initTableMapperJob(scans, mapper, outputKeyClass, outputValueClass, job,
        true);
  }

  // for multi-table
  public static void initTableMapperJob(List<Scan> scans,
      Class<? extends TableMapper> mapper,
      Class<? extends WritableComparable> outputKeyClass,
      Class<?> outputValueClass, Job job,
      boolean addDependencyJars) throws IOException {
    job.setInputFormatClass(MultiThemisTableInputFormat.class);
    if (outputValueClass != null) {
      job.setMapOutputValueClass(outputValueClass);
    }
    if (outputKeyClass != null) {
      job.setMapOutputKeyClass(outputKeyClass);
    }
    job.setMapperClass(mapper);
    HBaseConfiguration.addHbaseResources(job.getConfiguration());
    job.getConfiguration().setStrings("io.serializations",
      job.getConfiguration().get("io.serializations"), WritableSerialization.class.getName(),
      ResultSerialization.class.getName());
    List<String> scanStrings = new ArrayList<String>();

    for (Scan scan : scans) {
      scanStrings.add(convertScanToString(scan));
    }
    job.getConfiguration().setStrings(MultiTableInputFormat.SCANS,
      scanStrings.toArray(new String[scanStrings.size()]));

    if (addDependencyJars) {
      addDependencyJars(job);
    }
    TableMapReduceUtil.initCredentials(job);
  }
  
  // for reduce
  public static void initMultiTableReducerJob(Class<? extends Reducer> reducer,
      Job job) throws IOException {
    initTableReducerJob("", reducer, job, null, null, null, null, true,
      MultiThemisTableOutputFormat.class);
  }
  
  public static void initTableReducerJob(String table, Class<? extends Reducer> reducer,
      Job job) throws IOException {
    initTableReducerJob(table, reducer, job, null);
  }

  public static void initTableReducerJob(String table, Class<? extends Reducer> reducer,
      Job job, Class partitioner) throws IOException {
    initTableReducerJob(table, reducer, job, partitioner, null, null, null);
  }

  public static void initTableReducerJob(String table, Class<? extends Reducer> reducer,
      Job job, Class partitioner, String quorumAddress, String serverClass, String serverImpl)
      throws IOException {
    initTableReducerJob(table, reducer, job, partitioner, quorumAddress, serverClass, serverImpl,
      true, ThemisTableOutputFormat.class);
  }

  public static void initTableReducerJob(String table, Class<? extends Reducer> reducer,
      Job job, Class partitioner, String quorumAddress, String serverClass, String serverImpl,
      boolean addDependencyJars, Class<? extends OutputFormat> outputFormatClass) throws IOException {

    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
    job.setOutputFormatClass(outputFormatClass);
    if (reducer != null) job.setReducerClass(reducer);
    conf.set(TableOutputFormat.OUTPUT_TABLE, table);
    // If passed a quorum/ensemble address, pass it on to TableOutputFormat.
    if (quorumAddress != null) {
      // Calling this will validate the format
      ZKUtil.transformClusterKey(quorumAddress);
      conf.set(TableOutputFormat.QUORUM_ADDRESS, quorumAddress);
    }
    if (serverClass != null && serverImpl != null) {
      conf.set(TableOutputFormat.REGION_SERVER_CLASS, serverClass);
      conf.set(TableOutputFormat.REGION_SERVER_IMPL, serverImpl);
    }
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Writable.class);
    if (partitioner == HRegionPartitioner.class) {
      job.setPartitionerClass(HRegionPartitioner.class);
      HTable outputTable = new HTable(conf, table);
       int regions = MetaReader.getRegionCount(conf, table);
       if (job.getNumReduceTasks() > regions) {
         job.setNumReduceTasks(regions);
       }
    } else if (partitioner != null) {
      job.setPartitionerClass(partitioner);
    }

    if (addDependencyJars) {
      addDependencyJars(job);
    }

    TableMapReduceUtil.initCredentials(job);
  }
}
