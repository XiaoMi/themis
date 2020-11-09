package org.apache.hadoop.hbase.themis.config;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public class ZkClusterInfo {
  public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 11000;
  public static final int CLUSTER_TYPE_LEN = 3;
  private final String clusterName;
  private final ZkClusterInfo.ClusterType clusterType;
  private final int port;

  public ZkClusterInfo(String clusterName) throws IOException {
    this(clusterName, -1);
  }

  public ZkClusterInfo(String clusterName, int port) throws IOException {
    this.clusterName = clusterName;
    int length = clusterName.length();
    String clusterTypeName = clusterName.substring(length - 3, length);

    try {
      this.clusterType = ZkClusterInfo.ClusterType.valueOf(clusterTypeName.toUpperCase());
    } catch (IllegalArgumentException var6) {
      throw new IOException("Illegal zookeeper cluster type: " + clusterTypeName);
    }

    this.port = port == -1 ? 11000 : port;
  }

  public ZkClusterInfo.ClusterType getClusterType() {
    return this.clusterType;
  }

  public int getPort() {
    return this.port;
  }

  public String toDnsName() {
    return this.clusterName + ".observer.zk.hadoop.srv";
  }

  public String resolve() throws IOException {
    String dnsName = this.toDnsName();

    try {
      InetAddress[] allAddresses = InetAddress.getAllByName(dnsName);
      String[] allIps = new String[allAddresses.length];

      for(int i = 0; i < allAddresses.length; ++i) {
        allIps[i] = allAddresses[i].getHostAddress();
      }

      Arrays.sort(allIps);
      return StringUtils.join(allIps, ',');
    } catch (UnknownHostException var5) {
      throw new IOException("Failed to resolve dns name and it doesn't hava a back off: " + dnsName);
    }
  }

  public enum ClusterType {
    SRV,
    PRC,
    TST,
    SEC;

    private ClusterType() {
    }
  }
}
