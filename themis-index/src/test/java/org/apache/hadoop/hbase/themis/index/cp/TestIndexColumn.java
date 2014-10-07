package org.apache.hadoop.hbase.themis.index.cp;

import org.apache.hadoop.hbase.themis.TestBase;
import org.junit.Assert;
import org.junit.Test;

public class TestIndexColumn extends TestBase {
  @Test
  public void testConstructor() {
    IndexColumn indexColumn = new IndexColumn(TABLENAME, FAMILY, QUALIFIER);
    Assert.assertArrayEquals(TABLENAME, indexColumn.getTableName());
    Assert.assertArrayEquals(FAMILY, indexColumn.getFamily());
    Assert.assertArrayEquals(QUALIFIER, indexColumn.getQualifier());
  }
  
  @Test
  public void testEqualAndHashCode() {
    IndexColumn indexColumnA = new IndexColumn(TABLENAME, FAMILY, QUALIFIER);
    IndexColumn indexColumnB = new IndexColumn(TABLENAME, FAMILY, QUALIFIER);
    Assert.assertTrue(indexColumnA.equals(indexColumnB));
    Assert.assertEquals(indexColumnA.hashCode(), indexColumnB.hashCode());
    IndexColumn indexColumnC = new IndexColumn(TABLENAME, FAMILY, ANOTHER_FAMILY);
    Assert.assertFalse(indexColumnA.equals(indexColumnC));
    Assert.assertFalse(indexColumnA.hashCode() == indexColumnC.hashCode());
  }
}
