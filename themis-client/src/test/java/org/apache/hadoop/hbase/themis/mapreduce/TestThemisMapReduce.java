package org.apache.hadoop.hbase.themis.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.themis.ThemisGet;
import org.apache.hadoop.hbase.themis.ThemisPut;
import org.apache.hadoop.hbase.themis.Transaction;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class TestThemisMapReduce extends TestThemisMapReduceBase {
  protected void writeTestData() throws IOException {
    Transaction transaction = new Transaction(connection);
    for (byte[] tableName : new byte[][] { TABLENAME, ANOTHER_TABLENAME }) {
      transaction.put(tableName, new ThemisPut(ROW).add(FAMILY, QUALIFIER, Bytes.toBytes(1)));
      transaction.put(tableName,
        new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, Bytes.toBytes(1)));
    }
    transaction.commit();
  }
  
  public static class RowSumReducer extends
      TableReducer<ImmutableBytesWritable, Result, ImmutableBytesWritable> {
    public static byte[] getReduceRow(byte[] row) {
      return Bytes.toBytes("prefix" + Bytes.toString(row));
    }
    
    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context)
        throws IOException, InterruptedException {
      int totalValue = 0;
      for (Result result : values) {
        for (Cell kv : result.listCells()) {
          totalValue += Bytes.toInt(kv.getValueArray());
        }
      }
      byte[] newKey = getReduceRow(key.copyBytes());
      context.write(key, new Put(newKey).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(totalValue)));
    }
  }
  
  public static class PrefixCopyMapper extends TableMapper<ImmutableBytesWritable, TableMutations> {
    private byte[] tableName;
    
    @Override
    public void setup(Context context) {
      tableName = ((TableSplit)context.getInputSplit()).getTableName();
    }
    
    protected static byte[] getRowKeyWithPrefix(byte[] row) {
      return Bytes.add(Bytes.toBytes("multi-table-reducer-prefix-"), row);
    }
    
    public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException,
        InterruptedException {
      byte[] reduceKey = getRowKeyWithPrefix(row.copyBytes());
      Put put = new Put(reduceKey);
      for (Cell kv : values.listCells()) {
        put.addColumn(kv.getFamilyArray(), kv.getFamilyArray(), kv.getFamilyArray());
      }
      TableMutations tableMuation = new TableMutations(tableName);
      tableMuation.add(put);
      context.write(new ImmutableBytesWritable(reduceKey), tableMuation);
    }
  }
  
  @Test
  public void testThemisMapReduce() throws Exception {
    if (TEST_UTIL != null) {
      writeTestData();
      Job job = new Job(table.getConfiguration(), "testThemisMapReduce");
      job.setNumReduceTasks(1);
      Scan scan = new Scan();
      scan.addColumn(FAMILY, QUALIFIER);
      List<Scan> scans = new ArrayList<Scan>();
      for (byte[] tableName : new byte[][] { TABLENAME, ANOTHER_TABLENAME }) {
        Scan thisScan = new Scan(scan);
        thisScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName);
        scans.add(thisScan);
      }

      ThemisTableMapReduceUtil.initTableMapperJob(scans, IdentityTableMapper.class,
        ImmutableBytesWritable.class, Result.class, job);
      ThemisTableMapReduceUtil.initTableReducerJob(Bytes.toString(TABLENAME), RowSumReducer.class,
        job);
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(), RowSumReducer.class);
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(), TransactionTestBase.class);
      assertTrue(job.waitForCompletion(true));

      Transaction transaction = new Transaction(connection);
      Result result = transaction.get(TABLENAME,
        new ThemisGet(RowSumReducer.getReduceRow(ROW)).addColumn(FAMILY, QUALIFIER));
      Assert.assertEquals(2, Bytes.toInt(result.getValue(FAMILY, QUALIFIER)));
    }
  }
  
  @Test
  public void testMultiThemisTableOutputFormat() throws Exception {
    if (TEST_UTIL != null) {
      writeTestData();
      Job job = new Job(table.getConfiguration(), "testMultiThemisTableOutputFormat");
      job.setNumReduceTasks(1);
      Scan scan = new Scan();
      scan.addColumn(FAMILY, QUALIFIER);
      List<Scan> scans = new ArrayList<Scan>();
      for (byte[] tableName : new byte[][] { TABLENAME, ANOTHER_TABLENAME }) {
        Scan thisScan = new Scan(scan);
        thisScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName);
        scans.add(thisScan);
      }

      ThemisTableMapReduceUtil.initTableMapperJob(scans, PrefixCopyMapper.class,
        ImmutableBytesWritable.class, TableMutations.class, job);
      ThemisTableMapReduceUtil.initMultiTableReducerJob(MultiThemisTableReducer.class, job);
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(), PrefixCopyMapper.class);
      TableMapReduceUtil.addDependencyJars(job.getConfiguration(), TransactionTestBase.class);
      assertTrue(job.waitForCompletion(true));

      Transaction transaction = new Transaction(connection);
      for (byte[] row : new byte[][] { ROW, ANOTHER_ROW }) {
        Result resultA = transaction.get(TABLENAME,
          new ThemisGet(PrefixCopyMapper.getRowKeyWithPrefix(row)).addColumn(FAMILY, QUALIFIER));
        Result resultB = transaction.get(ANOTHER_TABLENAME,
          new ThemisGet(PrefixCopyMapper.getRowKeyWithPrefix(row)).addColumn(FAMILY, QUALIFIER));
        Assert.assertEquals(1, resultA.size());
        Assert.assertEquals(1, resultB.size());
        Assert.assertEquals(resultA.listCells().get(0).getTimestamp(), resultB.listCells().get(0)
            .getTimestamp());
      }
    }
  }
}
