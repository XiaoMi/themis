package org.apache.hadoop.hbase.themis.columns;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.annotations.VisibleForTesting;

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
  
  public static final String PUT_FAMILY_NAME = "#p";
  public static final byte[] PUT_FAMILY_NAME_BYTES = Bytes.toBytes(PUT_FAMILY_NAME);
  public static final String DELETE_FAMILY_NAME = "#d";
  public static final byte[] DELETE_FAMILY_NAME_BYTES = Bytes.toBytes(DELETE_FAMILY_NAME);
  public static final byte[][] COMMIT_FAMILY_NAME_BYTES = new byte[][] { PUT_FAMILY_NAME_BYTES,
      DELETE_FAMILY_NAME_BYTES };
  public static final String THEMIS_COMMIT_FAMILY_TYPE = "themis.commit.family.type";
  protected static CommitFamily commitFamily;

  public static enum CommitFamily {
    SAME_WITH_DATA_FAMILY,
      DIFFERNT_FAMILY
  }

  // auxiliary family and qualifier
  public static final String AUXILIARY_FAMILY_NAME_KEY = "themis.auxiliary.family.name";
  public static final String DEFAULT_AUXILIARY_FAMILY_NAME = "T";
  public static final String AUXILIARY_QUALIFIER_NAME_KEY = "themis.auxiliary.qualifier.name";
  public static final String DEFAULT_AUXILIARY_QUALIFIER_NAME = "__t";

  protected static String auxiliaryFamily;
  protected static byte[] auxiliaryFamilyBytes;
  protected static String auxiliaryQualifier;
  protected static byte[] auxiliaryQualifierBytes;

  public static void init(Configuration conf) {
    commitFamily = CommitFamily.valueOf(conf.get(THEMIS_COMMIT_FAMILY_TYPE,
      CommitFamily.DIFFERNT_FAMILY.toString()));
    auxiliaryFamily = conf.get(AUXILIARY_FAMILY_NAME_KEY, DEFAULT_AUXILIARY_FAMILY_NAME);
    auxiliaryFamilyBytes = Bytes.toBytes(auxiliaryFamily);
    auxiliaryQualifier = conf.get(AUXILIARY_QUALIFIER_NAME_KEY, DEFAULT_AUXILIARY_QUALIFIER_NAME);
    auxiliaryQualifierBytes = Bytes.toBytes(auxiliaryQualifier);
  }
  
  public static boolean isCommitToSameFamily() {
    return commitFamily == CommitFamily.SAME_WITH_DATA_FAMILY;
  }

  public static boolean isCommitToDifferentFamily() {
    return commitFamily == CommitFamily.DIFFERNT_FAMILY;
  }
  
  public static boolean isPreservedColumn(Column column) {
    return containPreservedCharacter(column) || isLockColumn(column) || isPutColumn(column)
        || isDeleteColumn(column);
  }
  
  public static boolean containPreservedCharacter(Column column) {
    for (int i = 0; i < column.getFamily().length; ++i) {
      if (PRESERVED_COLUMN_CHARACTER_BYTES[0] == column.getFamily()[i]) {
        return true;
      }
    }
    if (isCommitToSameFamily()) {
      for (int i = 0; i < column.getQualifier().length; ++i) {
        if (PRESERVED_COLUMN_CHARACTER_BYTES[0] == column.getQualifier()[i]) {
          return true;
        }
      }
    }
    return false;
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
  
  public static boolean isCommitFamily(byte[] family) {
    return Bytes.equals(PUT_FAMILY_NAME_BYTES, family)
        || Bytes.equals(DELETE_FAMILY_NAME_BYTES, family);
  }

  // judge the data/lock/write column
  public static boolean isLockColumn(byte[] family, byte[] qualifier) {
    return isLockColumn(new Column(family, qualifier));
  }
  
  public static boolean isPutColumn(byte[] family, byte[] qualifier) {
    return isPutColumn(new Column(family, qualifier));
  }
  
  public static boolean isPutColumn(Column column) {
    if (isCommitToSameFamily()) {
      return isQualifierWithSuffix(column.getQualifier(), PUT_QUALIFIER_SUFFIX_BYTES);
    } else {
      return Bytes.equals(PUT_FAMILY_NAME_BYTES, column.getFamily());
    }
  }

  public static boolean isDeleteColumn(Column column) {
    return isDeleteColumn(column.getFamily(), column.getQualifier());
  }
  
  public static boolean isDeleteColumn(byte[] family, byte[] qualifier) {
    if (isCommitToSameFamily()) {
      return isQualifierWithSuffix(qualifier, DELETE_QUALIFIER_SUFFIX_BYTES);
    } else {
      return Bytes.equals(DELETE_FAMILY_NAME_BYTES, family);
    }
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

  public static boolean isAuxiliaryFamily(byte[] family) {
    return Bytes.equals(family, auxiliaryFamilyBytes);
  }

  public static boolean isAuxiliaryColumn(Column column) {
    if (column.getFamily() == null) {
      return false;
    }

    if (Bytes.equals(auxiliaryFamilyBytes, column.getFamily())
        && Bytes.equals(auxiliaryQualifierBytes, column.getQualifier())) {
      return true;
    }
    return false;
  }
  
  // transfer among data/lock/write column
  public static Column getLockColumn(byte[] family, byte[] qualifier) {
    return getLockColumn(new Column(family, qualifier));
  }
  
  public static Column getLockColumn(Column dataColumn) {
    return new Column(LOCK_FAMILY_NAME, constructQualifierFromColumn(dataColumn));
  }

  public static Column getPutColumn(Column dataColumn) {
    if (isCommitToSameFamily()) {
      return new Column(dataColumn.getFamily(), concatQualifierWithSuffix(
        dataColumn.getQualifier(), PUT_QUALIFIER_SUFFIX_BYTES));
    } else {
      return new Column(PUT_FAMILY_NAME_BYTES, constructQualifierFromColumn(dataColumn));
    }
  }

  public static Column getDeleteColumn(Column dataColumn) {
    if (isCommitToSameFamily()) {
      return new Column(dataColumn.getFamily(), concatQualifierWithSuffix(
        dataColumn.getQualifier(), DELETE_QUALIFIER_SUFFIX_BYTES));
    } else {
      return new Column(DELETE_FAMILY_NAME_BYTES, constructQualifierFromColumn(dataColumn));
    }
  }

  public static Column getDataColumn(Column lockOrWriteColumn) {
    if (isCommitToSameFamily()) {
      if (isLockColumn(lockOrWriteColumn)) {
        return getDataColumnFromConstructedQualifier(lockOrWriteColumn);
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
    } else {
      return getDataColumnFromConstructedQualifier(lockOrWriteColumn);
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

  public static Column getDataColumnFromConstructedQualifier(Column lockColumn) {
    byte[] constructedQualifier = lockColumn.getQualifier();
    if (constructedQualifier == null) {
      // TODO : throw exception or log an error
      return lockColumn;
    }
    int index = -1;
    for (int i = 0; i < constructedQualifier.length; ++i) {
      // the first PRESERVED_COLUMN_CHARACTER_BYTES exist in lockQualifier is the delimiter
      if (PRESERVED_COLUMN_CHARACTER_BYTES[0] == constructedQualifier[i]) {
        index = i;
        break;
      }
    }
    if (index <= 0) {
      return lockColumn;
    } else {
      byte[] family = new byte[index];
      byte[] qualifier = new byte[constructedQualifier.length - index - 1];
      System.arraycopy(constructedQualifier, 0, family, 0, index);
      System.arraycopy(constructedQualifier, index + 1, qualifier, 0, constructedQualifier.length
          - index - 1);
      return new Column(family, qualifier);
    }
  }

  protected static byte[] constructQualifierFromColumn(Column column) {
    return Bytes.add(column.getFamily(), PRESERVED_COLUMN_CHARACTER_BYTES, column.getQualifier());
  }

  @VisibleForTesting
  public static String getAuxiliaryFamily() {
    return auxiliaryFamily;
  }

  @VisibleForTesting
  public static byte[] getAuxiliaryFamilyBytes() {
    return auxiliaryFamilyBytes;
  }

  @VisibleForTesting
  public static String getAuxiliaryQualifier() {
    return auxiliaryQualifier;
  }

  @VisibleForTesting
  public static byte[] getAuxiliaryQualifierBytes() {
    return auxiliaryQualifierBytes;
  }
}
