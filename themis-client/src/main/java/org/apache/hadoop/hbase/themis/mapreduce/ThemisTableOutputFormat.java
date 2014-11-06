package org.apache.hadoop.hbase.themis.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.themis.ThemisDelete;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ThemisTableOutputFormat<KEY> extends TableOutputFormat<KEY> {
  private static final Log LOG = LogFactory.getLog(ThemisTableOutputFormat.class);
  private Configuration conf;
  private byte[] tableName;
  
  protected static class ThemisTableRecordWriter<KEY> extends
      ThemisTableRecordWriterBase<KEY, Writable> {
    private byte[] tableName;
    
    public ThemisTableRecordWriter(byte[] tableName, Configuration conf) throws IOException {
      super(conf);
      this.tableName = tableName;
    }

    @Override
    public void doWrite(KEY key, Writable value, Transaction transaction) throws IOException,
        InterruptedException {
      if (value instanceof Put) {
        transaction.put(tableName, new ThemisPut((Put)value));
      } else if (value instanceof Delete) {
        transaction.delete(tableName, new ThemisDelete((Delete)value));
      } else {
        throw new IOException("Pass a Delete or a Put");
      }
    }
  }
  
  @Override
  public RecordWriter<KEY, Writable> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new ThemisTableRecordWriter<KEY>(tableName, conf);
  }
  
  @Override
  public void setConf(Configuration otherConf) {
    this.conf = HBaseConfiguration.create(otherConf);
    String tableName = this.conf.get(OUTPUT_TABLE);
    if(tableName == null || tableName.length() <= 0) {
      throw new IllegalArgumentException("Must specify table name");
    }
    String address = this.conf.get(QUORUM_ADDRESS);
    int zkClientPort = conf.getInt(QUORUM_PORT, 0);
    String serverClass = this.conf.get(REGION_SERVER_CLASS);
    String serverImpl = this.conf.get(REGION_SERVER_IMPL);
    try {
      if (address != null) {
        ZKUtil.applyClusterKeyToConf(this.conf, address);
      }
      if (serverClass != null) {
        this.conf.set(HConstants.REGION_SERVER_CLASS, serverClass);
        this.conf.set(HConstants.REGION_SERVER_IMPL, serverImpl);
      }
      if (zkClientPort != 0) {
        conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
      }
      this.tableName = Bytes.toBytes(tableName);
      LOG.info("Do Themis Write for table: "  + tableName);
    } catch(IOException e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
  }
}
