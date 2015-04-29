package org.apache.hadoop.hbase.themis.columns;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.themis.TestBase;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil.CommitFamily;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestColumnUtil extends TestBase {
  private static byte[] illegalQualifier = Bytes.toBytes("Qualifier#suffix");
  private static byte[] illegalLockQualifier = Bytes.add(FAMILY,
    ColumnUtil.PRESERVED_COLUMN_CHARACTER_BYTES, illegalQualifier);
  private Configuration conf = HBaseConfiguration.create();
 
  @Before
  public void initEnv() throws IOException {
    conf.set(ColumnUtil.THEMIS_COMMIT_FAMILY_TYPE, CommitFamily.SAME_WITH_DATA_FAMILY.toString());
    ColumnUtil.init(conf);
  }
  
  protected void commitToDifferentFamily() {
    conf.set(ColumnUtil.THEMIS_COMMIT_FAMILY_TYPE, CommitFamily.DIFFERNT_FAMILY.toString());
    ColumnUtil.init(conf);
  }
  
  @Test
  public void testIsPreservedColumn() {
    Assert.assertFalse(ColumnUtil.isPreservedColumn(COLUMN));
    Assert.assertTrue(ColumnUtil.isPreservedColumn(ColumnUtil.getLockColumn(COLUMN)));
    Assert.assertTrue(ColumnUtil.isPreservedColumn(ColumnUtil.getPutColumn(COLUMN)));
    Assert.assertTrue(ColumnUtil.isPreservedColumn(ColumnUtil.getDeleteColumn(COLUMN)));
    Assert.assertTrue(ColumnUtil.isPreservedColumn(new Column(Bytes.toBytes("fa#mily"), QUALIFIER)));
    
    commitToDifferentFamily();
    Assert.assertTrue(ColumnUtil.isPreservedColumn(new Column(Bytes.toBytes("fa#mily"), QUALIFIER)));
    Assert.assertFalse(ColumnUtil.isPreservedColumn(new Column(FAMILY, Bytes.toBytes("qu#alifier"))));
  }
  
  @Test
  public void testGetDataColumn() {
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(COLUMN, ColumnUtil.getDataColumn(COLUMN));
      Column column = new Column(COLUMN.getFamily(), COLUMN.getQualifier());
      Assert.assertEquals(column, ColumnUtil.getDataColumn(ColumnUtil.getLockColumn(COLUMN)));
      Assert.assertEquals(column, ColumnUtil.getDataColumn(ColumnUtil.getPutColumn(COLUMN)));
      Assert.assertEquals(column, ColumnUtil.getDataColumn(ColumnUtil.getDeleteColumn(COLUMN)));
      
      commitToDifferentFamily();
    }
  }
  
  @Test
  public void testConstructQualifierFromColumn() {
    byte[] expect = Bytes.toBytes("ThemisCF#Qualifier");
    byte[] actual = ColumnUtil.constructQualifierFromColumn(COLUMN);
    Assert.assertArrayEquals(expect, actual);
    actual = ColumnUtil.constructQualifierFromColumn(new Column(FAMILY, illegalQualifier));
    Assert.assertArrayEquals(illegalLockQualifier, actual);
  }

  @Test
  public void testGetLockColumn() {
    Column actual = ColumnUtil.getLockColumn(COLUMN);
    Column expect = new Column(ColumnUtil.LOCK_FAMILY_NAME, Bytes.toBytes("ThemisCF#Qualifier"));
    Assert.assertEquals(expect, actual);
    actual = ColumnUtil.getLockColumn(new Column(FAMILY, illegalQualifier));
    expect = new Column(ColumnUtil.LOCK_FAMILY_NAME, illegalLockQualifier);
    Assert.assertEquals(expect, actual);
  }

  @Test
  public void testGetDataColumnFromConstructedQualifier() {
    for (byte[] family : ColumnUtil.COMMIT_FAMILY_NAME_BYTES) {
      Column expect = new Column(FAMILY, QUALIFIER);
      Column actual = ColumnUtil.getDataColumnFromConstructedQualifier(expect);
      Assert.assertEquals(expect, actual);
      expect = new Column(family, QUALIFIER);
      actual = ColumnUtil.getDataColumnFromConstructedQualifier(expect);
      Assert.assertEquals(expect, actual);
      expect = new Column(FAMILY, QUALIFIER);
      actual = ColumnUtil.getDataColumnFromConstructedQualifier(new Column(family, Bytes
          .toBytes("ThemisCF#Qualifier")));
      Assert.assertEquals(expect, actual);
      expect = new Column(FAMILY, illegalQualifier);
      actual = ColumnUtil.getDataColumnFromConstructedQualifier(new Column(family,
          illegalLockQualifier));
      Assert.assertEquals(expect, actual);
    }
  }
  
  @Test
  public void testIsQualifierWithSuffix() {
    byte[] qualifier = Bytes.toBytes("");
    byte[] suffix = Bytes.toBytes("");
    Assert.assertTrue(ColumnUtil.isQualifierWithSuffix(qualifier, suffix));
    qualifier = Bytes.toBytes("c");
    Assert.assertTrue(ColumnUtil.isQualifierWithSuffix(qualifier, suffix));
    suffix = Bytes.toBytes("c");
    Assert.assertTrue(ColumnUtil.isQualifierWithSuffix(qualifier, suffix));
    suffix = Bytes.toBytes("d");
    Assert.assertFalse(ColumnUtil.isQualifierWithSuffix(qualifier, suffix));
    suffix = Bytes.toBytes("dc");
    Assert.assertFalse(ColumnUtil.isQualifierWithSuffix(qualifier, suffix));
  }
  
  @Test
  public void testContainPreservedCharacter() {
    Assert.assertTrue(ColumnUtil.containPreservedCharacter(new Column(Bytes.toBytes("#family"),
        Bytes.toBytes("qualifier"))));
    Assert.assertTrue(ColumnUtil.containPreservedCharacter(new Column(Bytes.toBytes("fam#ily"),
      Bytes.toBytes("qualifier"))));
    Assert.assertTrue(ColumnUtil.containPreservedCharacter(new Column(Bytes.toBytes("family#"),
      Bytes.toBytes("qualifier"))));
    Assert.assertTrue(ColumnUtil.containPreservedCharacter(new Column(Bytes.toBytes("family"),
        Bytes.toBytes("#qualifier"))));
    Assert.assertTrue(ColumnUtil.containPreservedCharacter(new Column(Bytes.toBytes("family"),
      Bytes.toBytes("qua#lifier"))));
    Assert.assertTrue(ColumnUtil.containPreservedCharacter(new Column(Bytes.toBytes("family"),
      Bytes.toBytes("qualifier#"))));
    Assert.assertFalse(ColumnUtil.containPreservedCharacter(new Column(Bytes.toBytes("family"),
        Bytes.toBytes("qualifier"))));
    byte[] qualifier = new byte[]{0x00, 0x00, 0x07, (byte)((int)0xDC)};
    Assert.assertFalse(ColumnUtil.containPreservedCharacter(new Column(Bytes.toBytes("family"),
        qualifier)));
    qualifier = new byte[] { 0x00, 0x00, 0x07, (byte) ((int) 0xDC),
        ColumnUtil.PRESERVED_COLUMN_CHARACTER_BYTES[0] };
    Assert.assertTrue(ColumnUtil.containPreservedCharacter(new Column(Bytes.toBytes("family"),
      qualifier)));
  }
  
  @Test
  public void testConcatQualifierWithSuffix() {
    byte[] actual = ColumnUtil.concatQualifierWithSuffix(QUALIFIER, ColumnUtil.PUT_QUALIFIER_SUFFIX_BYTES);
    Assert.assertEquals(QUALIFIER.length + ColumnUtil.PUT_QUALIFIER_SUFFIX.length(), actual.length);
    Assert.assertTrue(Bytes.equals(QUALIFIER, 0, QUALIFIER.length, actual, 0, QUALIFIER.length));
    
    byte[] from = new byte[]{0x00, 0x00, 0x07, (byte)((int)0xDC)};
    actual = ColumnUtil.concatQualifierWithSuffix(from, ColumnUtil.PUT_QUALIFIER_SUFFIX_BYTES);
    Assert.assertEquals(from.length + ColumnUtil.PUT_QUALIFIER_SUFFIX.length(), actual.length);
    Assert.assertTrue(Bytes.equals(from, 0, from.length, actual, 0, from.length));
  }
}
