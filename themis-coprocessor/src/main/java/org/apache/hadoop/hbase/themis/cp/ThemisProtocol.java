package org.apache.hadoop.hbase.themis.cp;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.themis.columns.ColumnMutation;

public interface ThemisProtocol {
  // return data result if no lock conflict or ignoreLock is set to true; otherwise, return the conflicted locks
  Result themisGet(final Get get, final long startTs, boolean ignoreLock) throws IOException;

  // prewrite a row. The row will contain primary column if primaryLock is not null, where primaryIndex is the
  // primary column in mutations. We don't pass secondaryLock for each secondary column because they are same
  // exception for the type which will be part of mutations
  byte[][] prewriteRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final byte[] secondaryLock, final byte[] primaryLock,
      final int primaryIndex) throws IOException;
  
  byte[][] prewriteSingleRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final byte[] secondaryLock, final byte[] primaryLock,
      final int primaryIndex) throws IOException;
  
  // commit for a row. The row will contain primary column if primaryIndex is not negative
  boolean commitRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final long commitTs, final int primaryIndex) throws IOException;

  boolean commitSingleRow(final byte[] row, final List<ColumnMutation> mutations,
      final long prewriteTs, final long commitTs, final int primaryIndex) throws IOException;
  
  boolean isLockExpired(final long lockTimestamp) throws IOException;
  
  // return null if lock not exist; otherwise, return lock and erase the lock
  byte[] getLockAndErase(final byte[] row, final byte[] family, final byte[] column,
      final long prewriteTs) throws IOException;
}
