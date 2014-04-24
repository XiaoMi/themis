package org.apache.hadoop.hbase.themis.columns;

import junit.framework.Assert;

import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestColumn {
  @Test
  public void testCompareTo() {
    byte[] a = Bytes.toBytes("a");
    byte[] b = Bytes.toBytes("b");
    Column columnA = new Column(a, b);
    Column columnB = new Column(a, b);
    Assert.assertEquals(0, columnA.compareTo(columnB));
    columnA = new Column(a, a);
    columnB = new Column(a, b);
    Assert.assertEquals(-1, columnA.compareTo(columnB));
    columnA = new Column(a, b);
    columnB = new Column(b, a);
    Assert.assertEquals(-1, columnA.compareTo(columnB));
    columnA = new Column(a, b);
    columnB = new Column(a, a);
    Assert.assertEquals(1, columnA.compareTo(columnB));
    columnA = new Column(b, a);
    columnB = new Column(a, b);
    Assert.assertEquals(1, columnA.compareTo(columnB));
  }
}
