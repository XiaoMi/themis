package org.apache.hadoop.hbase.themis.index.cp;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.themis.TestBase;
import org.junit.Test;

public class TestIndexColumn extends TestBase {

  @Test
  public void testConstructor() {
    IndexColumn indexColumn = new IndexColumn(TABLENAME, FAMILY, QUALIFIER);
    assertEquals(TABLENAME, indexColumn.getTableName());
    assertArrayEquals(FAMILY, indexColumn.getFamily());
    assertArrayEquals(QUALIFIER, indexColumn.getQualifier());
  }

  @Test
  public void testEqualAndHashCode() {
    IndexColumn indexColumnA = new IndexColumn(TABLENAME, FAMILY, QUALIFIER);
    IndexColumn indexColumnB = new IndexColumn(TABLENAME, FAMILY, QUALIFIER);
    assertTrue(indexColumnA.equals(indexColumnB));
    assertEquals(indexColumnA.hashCode(), indexColumnB.hashCode());
    IndexColumn indexColumnC = new IndexColumn(TABLENAME, FAMILY, ANOTHER_FAMILY);
    assertFalse(indexColumnA.equals(indexColumnC));
    assertFalse(indexColumnA.hashCode() == indexColumnC.hashCode());
  }
}
