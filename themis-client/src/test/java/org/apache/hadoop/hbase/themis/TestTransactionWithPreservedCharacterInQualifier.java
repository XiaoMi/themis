package org.apache.hadoop.hbase.themis;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTransactionWithPreservedCharacterInQualifier extends ClientTestBase {
  public static final byte[] QUALIFIER_P = Bytes.toBytes("Qualifer#p");
  public static final byte[] QUALIFIER_D = Bytes.toBytes("Qualifer#d");
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ClientTestBase.setUpBeforeClass();
    Transaction.init(conf);
  }
  
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    createTransactionWithMock();
  }
  
  @Test
  public void testPutDeleteGetScan() throws IOException {
    if (ColumnUtil.isCommitToDifferentFamily()) {
      // put
      ThemisPut put = new ThemisPut(ROW);
      put.add(FAMILY, QUALIFIER, VALUE);
      put.add(FAMILY, QUALIFIER_P, VALUE);
      put.add(ANOTHER_FAMILY, QUALIFIER_D, VALUE);
      transaction.put(TABLENAME, put);
      mockTimestamp(commitTs);
      transaction.commit();

      // get
      nextTransactionTs();
      createTransactionWithMock();
      ThemisGet get = new ThemisGet(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.addColumn(FAMILY, QUALIFIER_P);
      get.addColumn(ANOTHER_FAMILY, QUALIFIER_D);
      Result result = transaction.get(TABLENAME, get);
      Assert.assertEquals(3, result.size());
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER_P));
      Assert.assertArrayEquals(VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER_D));
      // get family
      get = new ThemisGet(ROW);
      get.addFamily(FAMILY);
      result = transaction.get(TABLENAME, get);
      Assert.assertEquals(2, result.size());
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER_P));
      // get whole row
      result = transaction.get(TABLENAME, new ThemisGet(ROW));
      Assert.assertEquals(3, result.size());
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER_P));
      Assert.assertArrayEquals(VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER_D));

      // delete
      ThemisDelete delete = new ThemisDelete(ROW);
      delete.deleteColumn(FAMILY, QUALIFIER_P);
      delete.deleteColumn(ANOTHER_FAMILY, QUALIFIER_D);
      transaction.delete(TABLENAME, delete);
      mockTimestamp(commitTs);
      transaction.commit();

      nextTransactionTs();
      createTransactionWithMock();
      get = new ThemisGet(ROW);
      result = transaction.get(TABLENAME, new ThemisGet(ROW));
      Assert.assertEquals(1, result.size());
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));

      // put again
      put = new ThemisPut(ROW);
      put.add(FAMILY, QUALIFIER_P, VALUE);
      put.add(ANOTHER_FAMILY, QUALIFIER_D, VALUE);
      transaction.put(TABLENAME, put);
      mockTimestamp(commitTs);
      transaction.commit();

      // scan
      nextTransactionTs();
      createTransactionWithMock();
      ThemisScan scan = new ThemisScan(ROW, ROW);
      scan.addColumn(FAMILY, QUALIFIER);
      scan.addColumn(FAMILY, QUALIFIER_P);
      scan.addColumn(ANOTHER_FAMILY, QUALIFIER_D);
      ResultScanner scanner = transaction.getScanner(TABLENAME, scan);
      result = scanner.next();
      Assert.assertEquals(3, result.size());
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER_P));
      Assert.assertArrayEquals(VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER_D));
      scanner.close();
      // scan family
      scan = new ThemisScan(ROW, ROW);
      scan.addFamily(FAMILY);
      scanner = transaction.getScanner(TABLENAME, scan);
      result = scanner.next();
      Assert.assertEquals(2, result.size());
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER_P));
      scanner.close();
      // scan whole row
      scanner = transaction.getScanner(TABLENAME, new ThemisScan(ROW, ROW));
      result = scanner.next();
      Assert.assertEquals(3, result.size());
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER_P));
      Assert.assertArrayEquals(VALUE, result.getValue(ANOTHER_FAMILY, QUALIFIER_D));
      scanner.close();

      // scan after delete
      delete = new ThemisDelete(ROW);
      delete.deleteColumn(FAMILY, QUALIFIER_P);
      delete.deleteColumn(ANOTHER_FAMILY, QUALIFIER_D);
      transaction.delete(TABLENAME, delete);
      mockTimestamp(commitTs);
      transaction.commit();

      nextTransactionTs();
      createTransactionWithMock();
      scanner = transaction.getScanner(TABLENAME, new ThemisScan(ROW, ROW));
      result = scanner.next();
      Assert.assertEquals(1, result.size());
      Assert.assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
      scanner.close();
    }
  }
}
