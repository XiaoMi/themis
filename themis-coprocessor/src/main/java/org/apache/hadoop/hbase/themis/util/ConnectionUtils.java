package org.apache.hadoop.hbase.themis.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionImplementation;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutorService;

public class ConnectionUtils {
  public static Connection createConnection(Configuration conf, ExecutorService pool, User user) throws IOException {
    if (user == null) {
      UserProvider provider = UserProvider.instantiate(conf);
      user = provider.getCurrent();
    }

    String className = conf.get("hbase.client.connection.impl", ConnectionImplementation.class.getName());

    Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException var7) {
      throw new IOException(var7);
    }

    try {
      Constructor<?> constructor = clazz.getDeclaredConstructor(Configuration.class, ExecutorService.class, User.class);
      constructor.setAccessible(true);
      return (Connection)constructor.newInstance(conf, pool, user);
    } catch (Exception var6) {
      throw new IOException(var6);
    }
  }
}
