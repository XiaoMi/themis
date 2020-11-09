package org.apache.hadoop.hbase.themis.util;

import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ObjectUtils {
  private ObjectUtils() {
    throw new RuntimeException("Can't create ObjectUtils instance");
  }

  public static void copyScan(Scan to, Scan from, List<Field> fields) throws IOException {
    try {
      for (Field field : fields) {
        field.setAccessible(true);
        copy(field, to, from);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static void copy(Field field, Scan to, Scan from) throws IllegalAccessException {
    Class<?> type = field.getType();
    if (type == int.class) {
      field.setInt(to, field.getInt(from));
    } else if (type == byte.class) {
      field.setByte(to, field.getByte(from));
    } else if (type == short.class) {
      field.setShort(to, field.getShort(from));
    } else if (type == long.class) {
      field.setLong(to, field.getLong(from));
    } else if (type == float.class) {
      field.setFloat(to, field.getFloat(from));
    } else if (type == double.class) {
      field.setDouble(to, field.getDouble(from));
    } else if (type == boolean.class) {
      field.setBoolean(to, field.getBoolean(from));
    } else  {
      field.set(to, field.get(from));
    }
  }

  public static void setObject(String fieldName, Scan to, Object value) throws IllegalAccessException {
    Field field = getField(Scan.class, fieldName);
    assert field != null;

    field.setAccessible(true);
    field.set(to, value);
  }

  public static void getAllField(Class<?> aClass, List<Field> fields) {
    Collections.addAll(fields, aClass.getDeclaredFields());
    if (aClass.getSuperclass() != null) {
      getAllField(aClass.getSuperclass(), fields);
    }
  }

  public static Field getField(Class<?> aClass, String fieldName) {
    Field field = null;
    try {
      field = aClass.getDeclaredField(fieldName);
    } catch (NoSuchFieldException | SecurityException e) {
      //ignore
    }

    if (Objects.nonNull(field)) {
      return field;
    }

    if (Objects.nonNull(aClass.getSuperclass())) {
      return getField(aClass.getSuperclass(), fieldName);
    }

    return null;
  }
}
