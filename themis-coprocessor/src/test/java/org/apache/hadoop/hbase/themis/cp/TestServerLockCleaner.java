package org.apache.hadoop.hbase.themis.cp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.lock.PrimaryLock;
import org.apache.hadoop.hbase.themis.lock.SecondaryLock;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

public class TestServerLockCleaner extends TransactionTestBase {
  protected ServerLockCleaner lockCleaner;

  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    lockCleaner = new ServerLockCleaner(connection, cpClient);
  }

  @Test
  public void testGetPrimaryLockWithColumn() throws IOException {
    PrimaryLock primary = (PrimaryLock) getLock(COLUMN);
    PrimaryLock actual = ServerLockCleaner.getPrimaryLockWithColumn(primary);
    assertTrue(primary.equals(actual));
    assertTrue(COLUMN.equals(actual.getColumn()));
    SecondaryLock secondary = (SecondaryLock) getLock(COLUMN_WITH_ANOTHER_ROW);
    actual = ServerLockCleaner.getPrimaryLockWithColumn(secondary);
    assertTrue(COLUMN.equals(actual.getColumn()));
    assertTrue(actual.isPrimary());
    assertNull(actual.getType());
    assertEquals(1, actual.getSecondaryColumns().size());
    assertNotNull(actual.getSecondaryColumns().get(COLUMN_WITH_ANOTHER_ROW));
  }

  @Test
  public void testCreateGetOfWriteColumnsIndexingPrewriteTs() throws IOException {
    Get get = lockCleaner.createGetOfWriteColumnsIndexingPrewriteTs(COLUMN, prewriteTs);
    TestThemisCpUtil.checkReadWithWriteColumns(get.getFamilyMap(), COLUMN);
    assertEquals(prewriteTs, get.getTimeRange().getMin());
    assertEquals(Long.MAX_VALUE, get.getTimeRange().getMax());
    assertEquals(Integer.MAX_VALUE, get.getMaxVersions());
  }

  @Test
  public void testGetTimestampOfWriteIndexingPrewriteTs() throws IOException {
    // write column is null
    assertNull(lockCleaner.getTimestampOfWriteIndexingPrewriteTs(COLUMN, prewriteTs));
    // value of write column not equal to prewriteTs
    writePutColumn(COLUMN, prewriteTs - 1, commitTs - 1);
    writePutColumn(COLUMN, prewriteTs + 1, commitTs + 1);
    writeDeleteColumn(COLUMN, prewriteTs + 2, commitTs + 2);
    assertNull(lockCleaner.getTimestampOfWriteIndexingPrewriteTs(COLUMN, prewriteTs));
    // value of write column equals to prewriteTs
    writePutColumn(COLUMN, prewriteTs, commitTs);
    Long ts = lockCleaner.getTimestampOfWriteIndexingPrewriteTs(COLUMN, prewriteTs);
    assertEquals(commitTs, ts.longValue());
    ts = lockCleaner.getTimestampOfWriteIndexingPrewriteTs(COLUMN, prewriteTs + 2);
    assertEquals(commitTs + 2, ts.longValue());
  }

  @Test
  public void testCleanPrimaryLock() throws IOException {
    // lock exist
    writeLockAndData(COLUMN);
    Pair<Long, PrimaryLock> result = lockCleaner.cleanPrimaryLock(COLUMN, prewriteTs);
    assertNull(result.getFirst());
    assertEquals(getLock(COLUMN), result.getSecond());
    assertNull(readLockBytes(COLUMN));
    // transaction committed with lock cleaned
    boolean[] isPuts = new boolean[] { true, false };
    for (boolean isPut : isPuts) {
      deleteOldDataAndUpdateTs();
      writeWriteColumn(COLUMN, prewriteTs, commitTs, isPut);
      result = lockCleaner.cleanPrimaryLock(COLUMN, prewriteTs);
      assertEquals(commitTs, result.getFirst().longValue());
      assertNull(result.getSecond());
    }
    // transaction uncommitted with lock erased
    deleteOldDataAndUpdateTs();
    result = lockCleaner.cleanPrimaryLock(COLUMN, prewriteTs);
    assertNull(result.getFirst());
    assertNull(result.getSecond());
  }

  protected void prepareCleanSecondaryLocks(boolean committed) throws IOException {
    if (committed) {
      writePutAndData(SECONDARY_COLUMNS[0], prewriteTs, commitTs);
    } else {
      writeData(SECONDARY_COLUMNS[0], prewriteTs);
    }
    for (int i = 1; i < SECONDARY_COLUMNS.length; ++i) {
      writeLockAndData(SECONDARY_COLUMNS[i]);
    }
  }

  @Test
  public void testEraseAndDataLock() throws IOException {
    writeLockAndData(COLUMN);
    lockCleaner.eraseLockAndData(COLUMN, prewriteTs);
    assertNotNull(readDataValue(COLUMN, prewriteTs));
    assertNull(readLockBytes(COLUMN));

    // erase lock by row
    for (ColumnCoordinate columnCoordinate : new ColumnCoordinate[] { COLUMN,
      COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER }) {
      writeLockAndData(columnCoordinate);
    }
    lockCleaner.eraseLockAndData(TABLENAME, PRIMARY_ROW.getRow(), PRIMARY_ROW.getColumns(),
      prewriteTs);
    for (ColumnCoordinate columnCoordinate : new ColumnCoordinate[] { COLUMN,
      COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER }) {
      if (getColumnType(columnCoordinate).equals(Type.Put)) {
        assertNotNull(readDataValue(columnCoordinate, prewriteTs));
      }
      assertNull(readLockBytes(columnCoordinate));
    }
  }

  @Test
  public void testCleanSecondaryLocks() throws IOException {
    // check commit secondary
    prepareCleanSecondaryLocks(true);
    lockCleaner.cleanSecondaryLocks((PrimaryLock) getLock(COLUMN), commitTs);
    checkCommitSecondariesSuccess();
    // check erase secondary lock
    deleteOldDataAndUpdateTs();
    prepareCleanSecondaryLocks(false);
    lockCleaner.cleanSecondaryLocks((PrimaryLock) getLock(COLUMN), null);
    checkSecondariesRollback();
  }

  @Test
  public void testCleanLockFromPrimary() throws IOException {
    // must set column before cleanLock
    ThemisLock from = getLock(COLUMN);
    // primary lock exist
    writeLockAndData(COLUMN);
    prepareCleanSecondaryLocks(false);
    lockCleaner.cleanLock(from);
    checkTransactionRollback();
    // transaction committed with primary lock cleaned
    deleteOldDataAndUpdateTs();
    writePutAndData(COLUMN, prewriteTs, commitTs);
    prepareCleanSecondaryLocks(true);
    from = getLock(COLUMN);
    lockCleaner.cleanLock(from);
    checkTransactionCommitSuccess();
    // transaction uncommitted with primary lock erased
    deleteOldDataAndUpdateTs();
    writeData(COLUMN, prewriteTs);
    prepareCleanSecondaryLocks(false);
    from = getLock(COLUMN);
    lockCleaner.cleanLock(from);
    checkTransactionRollback();
  }

  @Test
  public void testCleanLockFromSecondary() throws IOException {
    ColumnCoordinate columnCoordinate = SECONDARY_COLUMNS[1];
    ThemisLock from = getLock(columnCoordinate);
    // primary lock exist
    writeLockAndData(COLUMN);
    prepareCleanSecondaryLocks(false);
    lockCleaner.cleanLock(from);
    checkTransactionRollback();
    // transaction committed with primary lock cleaned
    deleteOldDataAndUpdateTs();
    writePutAndData(COLUMN, prewriteTs, commitTs);
    prepareCleanSecondaryLocks(true);
    from = getLock(columnCoordinate);
    lockCleaner.cleanLock(from);
    checkColumnsCommitSuccess(
      new ColumnCoordinate[] { COLUMN, SECONDARY_COLUMNS[0], SECONDARY_COLUMNS[1] });
    checkColumnsPrewriteSuccess(
      new ColumnCoordinate[] { SECONDARY_COLUMNS[2], SECONDARY_COLUMNS[3] });
    // transaction uncommitted with primary lock erased
    deleteOldDataAndUpdateTs();
    writeData(COLUMN, prewriteTs);
    prepareCleanSecondaryLocks(false);
    from = getLock(columnCoordinate);
    lockCleaner.cleanLock(from);
    checkColumnsRallback(
      new ColumnCoordinate[] { COLUMN, SECONDARY_COLUMNS[0], SECONDARY_COLUMNS[1] });
    checkColumnsPrewriteSuccess(
      new ColumnCoordinate[] { SECONDARY_COLUMNS[2], SECONDARY_COLUMNS[3] });
  }
}