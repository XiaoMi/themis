package org.apache.hadoop.hbase.themis.columns;

import org.apache.hadoop.hbase.util.Bytes;

public class ColumnUtil {
  public static final char PRESERVED_COLUMN_CHARACTER = '#'; // must check column family don't contain this character
  public static final byte[] PRESERVED_COLUMN_CHARACTER_BYTES = Bytes.toBytes("" + PRESERVED_COLUMN_CHARACTER);
  public static final String PUT_QUALIFIER_SUFFIX = PRESERVED_COLUMN_CHARACTER + "p";
  public static final byte[] PUT_QUALIFIER_SUFFIX_BYTES = Bytes.toBytes(PUT_QUALIFIER_SUFFIX);
  public static final String DELETE_QUALIFIER_SUFFIX = PRESERVED_COLUMN_CHARACTER + "d";
  public static final byte[] DELETE_QUALIFIER_SUFFIX_BYTES = Bytes.toBytes(DELETE_QUALIFIER_SUFFIX);
  public static final String PRESERVED_QUALIFIER_SUFFIX = PUT_QUALIFIER_SUFFIX + " or " + DELETE_QUALIFIER_SUFFIX;
  public static final byte[] LOCK_FAMILY_NAME = Bytes.toBytes("L");
  public static final String LOCK_FAMILY_NAME_STRING = Bytes.toString(LOCK_FAMILY_NAME);
  
  public static boolean isPreservedColumn(Column column) {
    return containPreservedCharacter(column) || isLockColumn(column) || isPutColumn(column)
        || isDeleteColumn(column);
  }
  
  public static boolean containPreservedCharacter(Column column) {
    return (Bytes.toString(column.getFamily()).indexOf(PRESERVED_COLUMN_CHARACTER) >= 0 ||
        Bytes.toString(column.getQualifier()).indexOf(PRESERVED_COLUMN_CHARACTER) >= 0);
  }
  
  public static boolean isLockColumn(Column column) {
    if (column.getFamily() == null) {
      return false;
    }
    if (Bytes.equals(LOCK_FAMILY_NAME, column.getFamily())) {
      return true;
    }
    return false;
  }

  // judge the data/lock/write column
  public static boolean isLockColumn(byte[] family, byte[] qualifier) {
    return isLockColumn(new Column(family, qualifier));
  }
  
  public static boolean isPutColumn(byte[] family, byte[] qualifier) {
    return isPutColumn(new Column(family, qualifier));
  }
  
  public static boolean isPutColumn(Column column) {
    return isQualifierWithSuffix(column.getQualifier(), PUT_QUALIFIER_SUFFIX_BYTES);
  }

  public static boolean isDeleteColumn(Column column) {
    return isQualifierWithSuffix(column.getQualifier(), DELETE_QUALIFIER_SUFFIX_BYTES);
  }
  
  public static boolean isWriteColumn(byte[] family, byte[] qualifier) {
    Column column = new Column(family, qualifier);
    return isWriteColumn(column);
  }
  
  public static boolean isWriteColumn(Column column) {
    return isPutColumn(column) || isDeleteColumn(column);
  }
  
  public static boolean isDataColumn(Column column) {
    return (!isLockColumn(column)) && (!isWriteColumn(column));
  }
  
  // transfer among data/lock/write column
  public static Column getLockColumn(byte[] family, byte[] qualifier) {
    return getLockColumn(new Column(family, qualifier));
  }
  
  public static Column getLockColumn(Column dataColumn) {
    return new Column(LOCK_FAMILY_NAME, constructLockColumnQualifier(dataColumn));
  }

  public static Column getPutColumn(Column dataColumn) {
    return new Column(dataColumn.getFamily(),
      concatQualifierWithSuffix(dataColumn.getQualifier(), PUT_QUALIFIER_SUFFIX_BYTES));
  }

  public static Column getDeleteColumn(Column dataColumn) {
    return new Column(dataColumn.getFamily(),
      concatQualifierWithSuffix(dataColumn.getQualifier(), DELETE_QUALIFIER_SUFFIX_BYTES));
  }

  public static Column getDataColumn(Column lockOrWriteColumn) {
    if (isLockColumn(lockOrWriteColumn)) {
      return getDataColumnFromLockColumn(lockOrWriteColumn);
    } else {
      byte[] qualifier = lockOrWriteColumn.getQualifier();
      if (isPutColumn(lockOrWriteColumn)) {
        return new Column(lockOrWriteColumn.getFamily(), Bytes.head(qualifier, qualifier.length
            - PUT_QUALIFIER_SUFFIX_BYTES.length));
      } else if (isDeleteColumn(lockOrWriteColumn)) {
        return new Column(lockOrWriteColumn.getFamily(), Bytes.head(qualifier, qualifier.length
            - DELETE_QUALIFIER_SUFFIX_BYTES.length));
      } else {
        return lockOrWriteColumn;
      }
    }
  }
  
  protected static byte[] concatQualifierWithSuffix(byte[] qualifier, byte[] suffix) {
    return qualifier == null ? qualifier : Bytes.add(qualifier, suffix);
  }

  // TODO : judge in which situation qualifier will be null ? null == ""?
  protected static boolean isQualifierWithSuffix(byte[] qualifier, byte[] suffix) {
    for (int i = 1; i <= suffix.length; ++i) {
      if (i > qualifier.length) {
        return false;
      }
      if (qualifier[qualifier.length - i] != suffix[suffix.length - i]) {
        return false;
      }
    }
    return true;
  }

  public static Column getDataColumnFromLockColumn(Column lockColumn) {
    String qualifierString = Bytes.toString(lockColumn.getQualifier());
    int index = qualifierString.indexOf(PRESERVED_COLUMN_CHARACTER);
    if (index <= 0) {
      // TODO : throw exception or log an error
      return lockColumn;
    } else {
      return new Column(Bytes.toBytes(qualifierString.substring(0, index)),
          Bytes.toBytes(qualifierString.substring(index + 1, qualifierString.length())));
    }
  }
  
  protected static byte[] constructLockColumnQualifier(Column column) {
    return Bytes.add(column.getFamily(), PRESERVED_COLUMN_CHARACTER_BYTES, column.getQualifier());
  }
}