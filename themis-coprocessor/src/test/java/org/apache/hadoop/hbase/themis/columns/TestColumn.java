package org.apache.hadoop.hbase.themis.columns;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestColumn {
  @Test
  public void testCompareTo() {
    byte[] a = Bytes.toBytes("a");
    byte[] b = Bytes.toBytes("b");
    Column columnA = new Column(a, b);
    Column columnB = new Column(a, b);
    assertEquals(0, columnA.compareTo(columnB));
    columnA = new Column(a, a);
    columnB = new Column(a, b);
    assertEquals(-1, columnA.compareTo(columnB));
    columnA = new Column(a, b);
    columnB = new Column(b, a);
    assertEquals(-1, columnA.compareTo(columnB));
    columnA = new Column(a, b);
    columnB = new Column(a, a);
    assertEquals(1, columnA.compareTo(columnB));
    columnA = new Column(b, a);
    columnB = new Column(a, b);
    assertEquals(1, columnA.compareTo(columnB));
  }
}
