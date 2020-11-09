package org.apache.hadoop.hbase.themis.config;


import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience.Private;

@Private
public class NameServiceEntry {
  private final URI uri;
  private final String scheme;
  private final ClusterInfo clusterInfo;
  private final String resource;

  public NameServiceEntry(URI uri) throws IOException {
    this.uri = uri;
    this.scheme = uri.getScheme();
    if (this.scheme == null) {
      this.clusterInfo = null;
    } else {
      this.clusterInfo = new ClusterInfo(uri.getHost(), uri.getPort());
      if (this.clusterInfo.isZkCluster() && !"zk".equals(this.scheme)) {
        throw new IOException("Illegal scheme, 'zk' expected: " + this.scheme);
      }
    }

    String path = uri.getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    this.resource = path;
  }

  public String getScheme() {
    return this.scheme;
  }

  public ClusterInfo getClusterInfo() {
    return this.clusterInfo;
  }

  public String getResource() {
    return this.resource;
  }

  public boolean isLocalResource() {
    return this.scheme == null;
  }

  public boolean compatibleWithScheme(String possibleScheme) {
    return this.scheme == null || this.scheme.equals(possibleScheme);
  }

  public Configuration createClusterConf(Configuration conf) throws IOException {
    if (this.isLocalResource()) {
      return conf;
    } else {
      conf = conf != null ? new Configuration(conf) : new Configuration();
      ZkClusterInfo zkClusterInfo = this.clusterInfo.getZkClusterInfo();
      conf.set("hbase.zookeeper.quorum", zkClusterInfo.resolve());
      conf.setInt("hbase.zookeeper.property.clientPort", zkClusterInfo.getPort());
      conf.set("zookeeper.znode.parent", "/hbase/" + this.clusterInfo.getClusterName());
      String kerberosPrinciple = "hbase_" + zkClusterInfo.getClusterType().toString().toLowerCase() + "/hadoop@XIAOMI.HADOOP";
      conf.set("hadoop.security.authentication", "kerberos");
      conf.set("hadoop.security.auth_to_local", "RULE:[1:$1] RULE:[2:$1] DEFAULT");
      conf.set("hbase.security.authentication", "kerberos");
      conf.set("hbase.master.kerberos.principal", kerberosPrinciple);
      conf.set("hbase.regionserver.kerberos.principal", kerberosPrinciple);
      return conf;
    }
  }
}

