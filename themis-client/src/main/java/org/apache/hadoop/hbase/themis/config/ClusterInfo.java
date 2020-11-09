package org.apache.hadoop.hbase.themis.config;

import java.io.IOException;

public class ClusterInfo {
  private final ZkClusterInfo zkClusterInfo;
  private final String clusterName;
  private final String clusterSuffixName;

  public ClusterInfo(String clusterName) throws IOException {
    this(clusterName, -1);
  }

  public ClusterInfo(String clusterName, int port) throws IOException {
    this.clusterName = clusterName;
    int pos = clusterName.indexOf(45);
    if (pos <= 0) {
      this.zkClusterInfo = new ZkClusterInfo(clusterName, port);
      this.clusterSuffixName = null;
    } else {
      this.zkClusterInfo = new ZkClusterInfo(clusterName.substring(0, pos), port);
      this.clusterSuffixName = clusterName.substring(pos + 1);
    }

  }

  public ZkClusterInfo getZkClusterInfo() {
    return this.zkClusterInfo;
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public boolean isZkCluster() {
    return this.clusterSuffixName == null;
  }
}
