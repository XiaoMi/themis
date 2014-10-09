package org.apache.hadoop.hbase.themis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.apache.hadoop.hbase.themis.columns.RowMutation;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.themis.lock.PrimaryLock;
import org.apache.hadoop.hbase.themis.lock.SecondaryLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Writable;

public class TestBase {
  protected static final String CLIENT_TEST_ADDRESS = "testAddress";
  protected static final byte[] TABLENAME = Bytes.toBytes("ThemisTable");
  protected static final byte[] ANOTHER_TABLENAME = Bytes.toBytes("AnotherThemisTable");
  protected static final byte[] ROW = Bytes.toBytes("Row");
  protected static final byte[] ANOTHER_ROW = Bytes.toBytes("AnotherRow");
  protected static final byte[] ZZ_ROW = Bytes.toBytes("ZZRow"); // sort after 'ROW' and
                                                                 // 'ANOTHER_ROW'
  protected static final byte[] FAMILY = Bytes.toBytes("ThemisCF");
  protected static final byte[] ANOTHER_FAMILY = Bytes.toBytes("AnotherThemisCF");
  protected static final byte[] QUALIFIER = Bytes.toBytes("Qualifier");
  protected static final byte[] ANOTHER_QUALIFIER = Bytes.toBytes("AnotherQualifier");
  protected static final byte[] VALUE = Bytes.toBytes("Value");
  protected static final byte[] ANOTHER_VALUE = Bytes.toBytes("AnotherValue");
  protected static final long PREWRITE_TS = System.currentTimeMillis();
  protected static final long COMMIT_TS = PREWRITE_TS + 10;
  protected static final KeyValue KEYVALUE = new KeyValue(ROW, FAMILY, QUALIFIER, PREWRITE_TS,
      Type.Put, VALUE);

  // define transaction columns used by unit test
  protected static final ColumnCoordinate COLUMN = new ColumnCoordinate(TABLENAME, ROW, FAMILY,
      QUALIFIER);
  protected static final ColumnCoordinate COLUMN_WITH_ANOTHER_TABLE = new ColumnCoordinate(
      ANOTHER_TABLENAME, ROW, FAMILY, QUALIFIER);
  protected static final ColumnCoordinate COLUMN_WITH_ANOTHER_ROW = new ColumnCoordinate(TABLENAME,
      ANOTHER_ROW, FAMILY, QUALIFIER);
  protected static final ColumnCoordinate COLUMN_WITH_ZZ_ROW = new ColumnCoordinate(TABLENAME,
      ZZ_ROW, FAMILY, QUALIFIER);
  protected static final ColumnCoordinate COLUMN_WITH_ANOTHER_FAMILY = new ColumnCoordinate(
      TABLENAME, ROW, ANOTHER_FAMILY, QUALIFIER);
  protected static final ColumnCoordinate COLUMN_WITH_ANOTHER_QUALIFIER = new ColumnCoordinate(
      TABLENAME, ROW, FAMILY, ANOTHER_QUALIFIER);

  // define column mutations of transaction used by test
  protected static final ColumnCoordinate[] SECONDARY_COLUMNS = new ColumnCoordinate[] {
      COLUMN_WITH_ANOTHER_TABLE, COLUMN_WITH_ANOTHER_ROW, COLUMN_WITH_ANOTHER_FAMILY,
      COLUMN_WITH_ANOTHER_QUALIFIER };
  // transaction by column
  protected static final ColumnCoordinate[] TRANSACTION_COLUMNS = new ColumnCoordinate[SECONDARY_COLUMNS.length + 1];
  private static final Map<ColumnCoordinate, Type> COLUMN_TYPES = new HashMap<ColumnCoordinate, Type>();
  static {
    TRANSACTION_COLUMNS[0] = COLUMN;
    for (int i = 0; i < SECONDARY_COLUMNS.length; ++i) {
      TRANSACTION_COLUMNS[i + 1] = SECONDARY_COLUMNS[i];
    }
    COLUMN_TYPES.put(COLUMN, Type.Put);
    COLUMN_TYPES.put(COLUMN_WITH_ANOTHER_TABLE, Type.Put);
    COLUMN_TYPES.put(COLUMN_WITH_ANOTHER_ROW, Type.DeleteColumn);
    COLUMN_TYPES.put(COLUMN_WITH_ANOTHER_FAMILY, Type.Put);
    COLUMN_TYPES.put(COLUMN_WITH_ANOTHER_QUALIFIER, Type.DeleteColumn);
  }

  protected static Type getColumnType(ColumnCoordinate columnCoordinate) {
    Type type = COLUMN_TYPES.get(columnCoordinate);
    return type == null ? Type.Put : type;
  }

  // transaction by row
  protected static RowMutation PRIMARY_ROW;
  protected static ColumnCoordinate[] PRIMARY_ROW_COLUMNS;
  protected static List<Pair<byte[], RowMutation>> SECONDARY_ROWS;
  static {
    PRIMARY_ROW = new RowMutation(ROW);
    PRIMARY_ROW_COLUMNS = new ColumnCoordinate[3];
    addToRowMutation(PRIMARY_ROW, COLUMN);
    PRIMARY_ROW_COLUMNS[0] = COLUMN;
    addToRowMutation(PRIMARY_ROW, COLUMN_WITH_ANOTHER_FAMILY);
    PRIMARY_ROW_COLUMNS[1] = COLUMN_WITH_ANOTHER_FAMILY;
    addToRowMutation(PRIMARY_ROW, COLUMN_WITH_ANOTHER_QUALIFIER);    
    PRIMARY_ROW_COLUMNS[2] = COLUMN_WITH_ANOTHER_QUALIFIER;
    SECONDARY_ROWS = new ArrayList<Pair<byte[], RowMutation>>();
    RowMutation secondaryRow = new RowMutation(ROW);
    addToRowMutation(secondaryRow, COLUMN_WITH_ANOTHER_TABLE);
    SECONDARY_ROWS.add(new Pair<byte[], RowMutation>(ANOTHER_TABLENAME, secondaryRow));
    secondaryRow = new RowMutation(ANOTHER_ROW);
    addToRowMutation(secondaryRow, COLUMN_WITH_ANOTHER_ROW);
    SECONDARY_ROWS.add(new Pair<byte[], RowMutation>(TABLENAME, secondaryRow));
  }
  
  public static void addToRowMutation(RowMutation rowMutation, ColumnCoordinate column) {
    Type type = getColumnType(column);
    rowMutation.addMutation(column, type, VALUE);
  }
  
  // construct primary and secondary lock
  public static PrimaryLock getPrimaryLock() {
    return getPrimaryLock(PREWRITE_TS);
  }

  public static PrimaryLock getPrimaryLock(long prewriteTs) {
    return getPrimaryLock(prewriteTs, false);
  }
  
  public static PrimaryLock getPrimaryLock(long prewriteTs, boolean singleRowTransaction) {
    PrimaryLock lock = new PrimaryLock(getColumnType(COLUMN));
    setThemisLock(lock, prewriteTs);
    ColumnCoordinate[] secondaryColumns = singleRowTransaction ? new ColumnCoordinate[] {
        COLUMN_WITH_ANOTHER_FAMILY, COLUMN_WITH_ANOTHER_QUALIFIER } : SECONDARY_COLUMNS;
    for (ColumnCoordinate columnCoordinate : secondaryColumns) {
      lock.addSecondaryColumn(columnCoordinate, getColumnType(columnCoordinate));
    }
    return lock;
  }

  public static SecondaryLock getSecondaryLock(ColumnCoordinate columnCoordinate) {
    return getSecondaryLock(getColumnType(columnCoordinate), PREWRITE_TS);
  }

  public static SecondaryLock getSecondaryLock(ColumnCoordinate columnCoordinate, long prewriteTs) {
    return getSecondaryLock(getColumnType(columnCoordinate), prewriteTs);
  }

  public static SecondaryLock getSecondaryLock(Type type, long prewriteTs) {
    SecondaryLock lock = new SecondaryLock(type);
    setThemisLock(lock, prewriteTs);
    lock.setPrimaryColumn(COLUMN);
    return lock;
  }

  public static void setThemisLock(ThemisLock lock, long prewriteTs) {
    lock.setTimestamp(prewriteTs);
    lock.setClientAddress(CLIENT_TEST_ADDRESS);
  }

  // construct lock / put / delete columns
  protected static KeyValue getLockKv(KeyValue dataKv) {
    return getLockKv(dataKv, VALUE);
  }
  
  protected static KeyValue getLockKv(KeyValue dataKv, byte[] lockBytes) {
    Column lockColumn = ColumnUtil.getLockColumn(new Column(dataKv.getFamily(), dataKv
      .getQualifier()));
    return new KeyValue(dataKv.getRow(), lockColumn.getFamily(), lockColumn.getQualifier(),
        PREWRITE_TS, Type.Put, lockBytes);
  }

  protected static KeyValue getPutKv(KeyValue dataKv) {
    Column putColumn = ColumnUtil
        .getPutColumn(new Column(dataKv.getFamily(), dataKv.getQualifier()));
    return getKeyValue(new ColumnCoordinate(ROW, putColumn.getFamily(), putColumn.getQualifier()),
      PREWRITE_TS);
  }

  public static KeyValue getPutKv(ColumnCoordinate column, long ts) {
    Column putColumn = ColumnUtil.getPutColumn(column);
    ColumnCoordinate wc = new ColumnCoordinate(column.getTableName(), column.getRow(), putColumn);
    return getKeyValue(wc, ts);
  }

  public static KeyValue getPutKv(ColumnCoordinate column, long prewriteTs, long commitTs) {
    Column wc = ColumnUtil.getPutColumn(column);
    return new KeyValue(column.getRow(), wc.getFamily(), wc.getQualifier(), commitTs, Type.Put,
        Bytes.toBytes(prewriteTs));
  }

  public static KeyValue getDeleteKv(ColumnCoordinate column, long ts) {
    Column deleteColumn = new Column(column.getFamily(), column.getQualifier());
    ColumnCoordinate dc = new ColumnCoordinate(column.getTableName(), column.getRow(), deleteColumn);
    return getKeyValue(dc, ts);
  }

  public static KeyValue getKeyValue(ColumnCoordinate c, long ts) {
    return new KeyValue(c.getRow(), c.getFamily(), c.getQualifier(), ts, Type.Put, VALUE);
  }

  // test Writable implementation
  public static void writeObjectToBufferAndRead(Writable expect, Writable actual)
      throws IOException {
    ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
    expect.write(new DataOutputStream(byteOutStream));
    ByteArrayInputStream byteInStream = new ByteArrayInputStream(byteOutStream.toByteArray());
    actual.readFields(new DataInputStream(byteInStream));
  }
  
  public static Column getColumn(ColumnCoordinate columnCoordinate) {
    return new Column(columnCoordinate.getFamily(), columnCoordinate.getQualifier());
  }
}