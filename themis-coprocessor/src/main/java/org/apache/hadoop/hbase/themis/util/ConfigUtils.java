package org.apache.hadoop.hbase.themis.util;

import com.google.common.base.Preconditions;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.themis.exception.HException;
import org.apache.hadoop.hbase.util.Pair;

import java.io.ByteArrayInputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConfigUtils {
  private static final Log LOG = LogFactory.getLog(ConfigUtils.class);
  public static final String ZK_PREFIX = "zk://";
  public static final String LOCAL_FILE_PREFIX = "file:///";
  public static final int ZK_SESSION_TIMEOUT = 30000;
  public static final int ZK_CONNECTION_TIMEOUT = 30000;
  public static final String SLASH = "/";
  public static final String HBASE_BUSINESS_ROOT_NODE = "/databases/hbase";
  public static final String HBASE_MICLOUD_GALLERY_ALBUMSHARETAG_NODE = "micloud/gallery_albumsharetag";
  public static final String HBASE_MICLOUDE_NODE = "micloud";
  public static final String HBASE_MILIAO_NODE = "miliao";
  public static final String HBASE_INFRA_NODE = "infra";
  public static final String HBASE_MICLOUD_SMS = "micloud/sms";
  public static final String HBASE_MICLOUD_PHONECALL = "micloud/phonecall";
  public static final String HBASE_MICLOUD_SMS_SEARCH = "micloud/sms_search";
  public static final String HBASE_XMPUSH = "xmpush";
  public static final String HBASE_URI_PREFIX = "hbase://";
  public static final int HBASE_URI_PREFIX_LENGTH = "hbase://".length();
  public static final String HTABLE_POOL_MAX_SIZE = "hbase.client.tablepool.maxsize";
  public static final String HBASE_CLIENT_SCANNER_CACHING = "hbase.client.scanner.caching";
  public static final String HBASE_IPC_CLIENT_CONNECTION_MAXIDELTIME = "hbase.ipc.client.connection.maxidletime";
  public static final String ZK_RECOVERY_RETRY = "zookeeper.recovery.retry";
  public static final String HTABLE_AUTOFLUSH = "hbase.client.autoflush";
  public static final String HBASE_CLUSTER_NAME = "hbase.cluster.name";
  public static final String HBASE_WRITE_ENABLED = "hbase.client.write.enabled";
  public static final String HBASE_READ_ENABLED = "hbase.client.read.enabled";
  public static final String HBASE_ADMIN_ENABLED = "hbase.client.admin.enabled";
  public static final String HBASE_SLOW_ACCESS_CUTOFF = "hbase.slow.access.cutoff";
  public static final String HBASE_IPC_CLIENT_TPCNODELAY = "hbase.ipc.client.tcpnodelay";
  public static final String HBASE_IPC_CREATE_SOCKET_TIMEOUT = "ipc.socket.timeout";
  public static final int DEFAULT_HBASE_SLOW_ACCESS_CUTOFF = 300;
  public static final String REPLICATION_ID_KEY = "hbase.client.replication.id";
  public static final String DEFAULT_REPLICATION_ID_KEY = "10";
  public static final String HADOOP_CLIENT_KRB_PRINCIPAL = "hadoop.client.kerberos.principal";
  public static final String HADOOP_CLIENT_KRB_PASSWD = "hadoop.client.kerberos.password";
  public static final String HADOOP_CLIENT_KRB_PASSWD_DECRYPTOR_CLASS = "hadoop.client.kerberos.password.decryptor";
  public static final String[] HBASE_CLIENT_CONFIG_KEYS = new String[]{"hbase.client.ipc.pool.size", "hbase.rpc.timeout", "hbase.client.operation.timeout", "hbase.client.pause", "hbase.client.retries.number", "hbase.client.scanner.caching", "hbase.ipc.client.connection.maxidletime", "zookeeper.session.timeout", "zookeeper.recovery.retry", "hbase.client.tablepool.maxsize", "hbase.zookeeper.quorum", "hbase.zookeeper.property.clientPort", "hbase.cluster.name", "hbase.client.write.enabled", "hbase.client.read.enabled", "hbase.client.autoflush", "hbase.slow.access.cutoff", "hbase.ipc.client.tcpnodelay", "ipc.socket.timeout", "hadoop.client.kerberos.principal", "hadoop.client.kerberos.password", "hadoop.client.kerberos.password.decryptor"};
  private static ConcurrentMap<String, ZkClient> zkClients = new ConcurrentHashMap();

  private static ZkClient getZkClient(String servers) {
    ZkClient client = zkClients.get(servers);
    if (client == null) {
      client = new ZkClient(servers, 30000, 30000, new BytesPushThroughSerializer());
      ZkClient current = zkClients.putIfAbsent(servers, client);
      if (current != null) {
        client.close();
        client = current;
      }
    }

    return client;
  }


  protected static Pair<String, String> getZkServerAndPath(String zkUri) throws HException {
    try {
      String tempZkUri = zkUri.substring(5);
      int firstSlashIndex = tempZkUri.indexOf("/");
      String server = tempZkUri.substring(0, firstSlashIndex);
      String path = tempZkUri.substring(firstSlashIndex);
      return Pair.newPair(server, path);
    } catch (Exception var5) {
      throw new HException(var5);
    }
  }

  protected static String getParentZkPath(String path) {
    Preconditions.checkArgument(!path.endsWith("/"), "Invalid zk node path: " + path);
    int pos = path.lastIndexOf("/");
    return pos == -1 ? null : path.substring(0, pos);
  }

  protected static byte[] loadConfigFromZK(String zkUri) throws HException {
    Pair<String, String> zkServerAndPath = getZkServerAndPath(zkUri);
    String server = zkServerAndPath.getFirst();
    String path = zkServerAndPath.getSecond();
    LOG.info("HBase load client information from zkServer=" + server + ", zkPath=" + path);
    ZkClient client = getZkClient(server);
    return (byte[])client.readData(path);
  }

  protected static Configuration loadHBaseConfigFromZK(String zkUri, boolean loadPrentPath) throws HException {
    Configuration conf = HBaseConfiguration.create();
    if (loadPrentPath) {
      conf.addResource(new ByteArrayInputStream(loadConfigFromZK(getParentZkPath(zkUri))));
    }

    conf.addResource(new ByteArrayInputStream(loadConfigFromZK(zkUri)));
    return conf;
  }

  public static boolean isZkPath(String path) {
    return path.startsWith("zk://");
  }

  public static boolean isLocalFile(String path) {
    return path.startsWith("file:///");
  }

  protected static void traceConfigChangeFromZk(final String zkUri, final boolean loadParentZkPath, final ConfigUtils.ConfigurationListener listener) throws HException {
    if (listener != null) {
      Pair<String, String> zkServerAndPath = getZkServerAndPath(zkUri);
      String path = zkServerAndPath.getSecond();
      ZkClient zkClient = getZkClient(zkServerAndPath.getFirst());
      LOG.info("trace config change from zk server = " + zkServerAndPath.getFirst() + ", path = " + path);
      IZkDataListener zkDataListener = new IZkDataListener() {
        @Override
        public void handleDataChange(String s, Object o) throws Exception {
          ConfigUtils.LOG.info("hbase configuration content updated, start to reload configuration");
          Configuration conf = ConfigUtils.loadHBaseConfigFromZK(zkUri, loadParentZkPath);
          listener.onChanged(conf);
        }

        @Override
        public void handleDataDeleted(String s) throws Exception {
        }
      };
      zkClient.subscribeDataChanges(path, zkDataListener);
      if (loadParentZkPath) {
        zkClient.subscribeDataChanges(getParentZkPath(path), zkDataListener);
      }
    }

  }

  public static Configuration loadConfiguration(String configPath, boolean loadParentZkPath) throws HException {
    try {
      Configuration configInPath;
      if (isZkPath(configPath)) {
        configInPath = loadHBaseConfigFromZK(configPath, loadParentZkPath);
      } else {
        if (!isLocalFile(configPath)) {
          throw new HException("configPath format error, be local file format, file://var/.. or zk path format: zk://host:port/path, actual=" + configPath);
        }

        configInPath = HBaseConfiguration.create();
        configInPath.addResource(new Path(configPath));
      }

      return configInPath;
    } catch (Throwable var3) {
      if (var3 instanceof HException) {
        throw (HException)var3;
      } else {
        throw new HException(var3);
      }
    }
  }

  public static Configuration loadConfiguration(String configPath) throws HException {
    return loadConfiguration(configPath, false);
  }

  public interface ConfigurationListener {
    /**
     * When config changed
     * @param var1
     */
    void onChanged(Configuration var1);
  }
}
