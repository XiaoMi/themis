package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.themis.ThemisDelete;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThemisTableOutputFormat<KEY> extends OutputFormat<KEY, Mutation>
    implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(ThemisTableOutputFormat.class);
  private Configuration conf;
  private TableName tableName;

  protected static class ThemisTableRecordWriter<KEY>
      extends ThemisTableRecordWriterBase<KEY, Mutation> {
    private TableName tableName;

    public ThemisTableRecordWriter(TableName tableName, Configuration conf) throws IOException {
      super(conf);
      this.tableName = tableName;
    }

    @Override
    public void doWrite(KEY key, Mutation value, Transaction transaction)
        throws IOException, InterruptedException {
      if (value instanceof Put) {
        transaction.put(tableName, new ThemisPut((Put) value));
      } else if (value instanceof Delete) {
        transaction.delete(tableName, new ThemisDelete((Delete) value));
      } else {
        throw new IOException("Pass a Delete or a Put");
      }
    }
  }

  @Override
  public RecordWriter<KEY, Mutation> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new ThemisTableRecordWriter<KEY>(tableName, conf);
  }

  @Override
  public void setConf(Configuration otherConf) {
    this.conf = HBaseConfiguration.create(otherConf);

    String tableName = this.conf.get(TableOutputFormat.OUTPUT_TABLE);
    if (tableName == null || tableName.length() <= 0) {
      throw new IllegalArgumentException("Must specify table name");
    }

    String address = this.conf.get(TableOutputFormat.QUORUM_ADDRESS);
    int zkClientPort = this.conf.getInt(TableOutputFormat.QUORUM_PORT, 0);
    String serverClass = this.conf.get(TableOutputFormat.REGION_SERVER_CLASS);
    String serverImpl = this.conf.get(TableOutputFormat.REGION_SERVER_IMPL);

    try {
      if (address != null) {
        ZKUtil.applyClusterKeyToConf(this.conf, address);
      }
      if (serverClass != null) {
        this.conf.set(HConstants.REGION_SERVER_IMPL, serverImpl);
      }
      if (zkClientPort != 0) {
        this.conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
      }
      this.tableName = TableName.valueOf(tableName);
      LOG.info("Created table instance for " + tableName);
    } catch (IOException e) {
      LOG.error("", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    // we can't know ahead of time if it's going to blow up when the user
    // passes a table name that doesn't exist, so nothing useful here.
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new TableOutputCommitter();
  }
}
