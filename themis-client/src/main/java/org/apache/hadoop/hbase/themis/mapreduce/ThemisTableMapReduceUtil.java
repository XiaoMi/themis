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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpointClient;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

public class ThemisTableMapReduceUtil {
  static Log LOG = LogFactory.getLog(TableMapReduceUtil.class);
  
  public static void initTableMapperJob(String table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<? extends WritableComparable> outputKeyClass,
      Class<? extends Writable> outputValueClass, Job job)
  throws IOException {
    initTableMapperJob(table, scan, mapper, outputKeyClass, outputValueClass,
        job, true);
  }


   public static void initTableMapperJob(byte[] table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<? extends WritableComparable> outputKeyClass,
      Class<? extends Writable> outputValueClass, Job job)
  throws IOException {
      initTableMapperJob(Bytes.toString(table), scan, mapper, outputKeyClass, outputValueClass,
              job, true);
  }

  public static void initTableMapperJob(String table, Scan scan,
      Class<? extends TableMapper> mapper, Class<? extends WritableComparable> outputKeyClass,
      Class<? extends Writable> outputValueClass, Job job, boolean addDependencyJars)
      throws IOException {
    initTableMapperJob(table, scan, mapper, outputKeyClass, outputValueClass, job,
      addDependencyJars, ThemisTableInputFormat.class);
  }
   
  public static void initTableMapperJob(String table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<? extends WritableComparable> outputKeyClass,
      Class<? extends Writable> outputValueClass, Job job,
      boolean addDependencyJars, Class<? extends InputFormat> inputFormatClass)
  throws IOException {
    job.setInputFormatClass(inputFormatClass);
    if (outputValueClass != null) job.setMapOutputValueClass(outputValueClass);
    if (outputKeyClass != null) job.setMapOutputKeyClass(outputKeyClass);
    job.setMapperClass(mapper);
    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
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
      Class<? extends Writable> outputValueClass, Job job) throws IOException {
    initTableMapperJob(scans, mapper, outputKeyClass, outputValueClass, job,
        true);
  }

  // for multi-table
  public static void initTableMapperJob(List<Scan> scans,
      Class<? extends TableMapper> mapper,
      Class<? extends WritableComparable> outputKeyClass,
      Class<? extends Writable> outputValueClass, Job job,
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
}