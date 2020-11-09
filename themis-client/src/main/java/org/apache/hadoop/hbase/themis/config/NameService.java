package org.apache.hadoop.hbase.themis.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class NameService {
  public static final String HBASE_URI_PREFIX = "hbase://";
  private static boolean hasCheckKerberos = false;

  public NameService() {
  }

  public static NameServiceEntry resolve(String resource) throws IOException {
    return resolve(resource, new Configuration());
  }

  public static NameServiceEntry resolve(String resource, Configuration conf) throws IOException {
    try {
      URI uri = new URI(resource);
      return resolve(uri, conf);
    } catch (URISyntaxException var3) {
      throw new IOException("Illegal uri: " + var3.getMessage());
    }
  }

  public static NameServiceEntry resolve(URI uri) throws IOException {
    return resolve(uri, new Configuration());
  }

  public static NameServiceEntry resolve(URI uri, Configuration conf) throws IOException {
    NameServiceEntry entry = new NameServiceEntry(uri);
    if (!entry.isLocalResource()) {
      checkKerberos(conf);
    }

    return entry;
  }

  private static synchronized void checkKerberos(Configuration conf) throws IOException {
    if (!hasCheckKerberos) {
      hasCheckKerberos = true;
      String value = conf.get("hadoop.security.authentication");
      if (!"kerberos".equals(value)) {
        throw new RuntimeException("Kerberos is required for name service, please make sure set hadoop.security.authentication to kerberos in the core-site.xml, or add -Dhadoop.property.hadoop.security.authentication=kerberos on JVM startup options.");
      }
    }

  }

  public static TableName resolveTableName(String fullTableName) {
    NameServiceEntry tableEntry = null;
    if (fullTableName.startsWith("hbase://")) {
      try {
        tableEntry = resolve(fullTableName);
        if (!tableEntry.compatibleWithScheme("hbase")) {
          throw new IOException("Unrecognized scheme: " + tableEntry.getScheme());
        }
      } catch (IOException var3) {
        throw new IllegalArgumentException("Given full table name is illegal", var3);
      }
    }

    return TableName.valueOf(tableEntry == null ? fullTableName : tableEntry.getResource());
  }

  public static String resolveClusterUri(String tableName) {
    return tableName.startsWith("hbase://") ? tableName.substring(0, tableName.lastIndexOf("/")) : "";
  }

  public static Configuration createConfigurationByClusterKey(String key) throws IOException {
    return createConfigurationByClusterKey(key, new Configuration());
  }

  public static Configuration createConfigurationByClusterKey(String key, Configuration conf) throws IOException {
    NameServiceEntry entry = resolve(key, conf);
    if (!entry.compatibleWithScheme("hbase")) {
      throw new IOException("Unrecognized scheme: " + entry.getScheme());
    } else {
      return entry.createClusterConf(conf);
    }
  }
}
