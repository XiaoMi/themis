package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.themis.lockcleaner.ClientName.ClientNameWithProcessId;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

public class ZookeeperWorkerRegister extends WorkerRegister implements Closeable {
  private static final Log LOG = LogFactory.getLog(ZookeeperWorkerRegister.class);
  public static final String THEMIS_ROOT_NODE = "/themis";
  public static final String ALIVE_CLIENT_NODE = "aliveclient";
      
  class ClientTracker extends ZooKeeperListener {
    public ClientTracker(ZooKeeperWatcher watcher) {
      super(watcher);
    }
    
    public void start() throws KeeperException, IOException {
      watcher.registerListener(this);
      loadAliveClients();
    }
    
    protected void loadAliveClients() throws KeeperException, IOException {
      aliveClients.clear();
      aliveClients.addAll(ZKUtil.listChildrenAndWatchThem(watcher, aliveClientParentPath));
    }
    
    @Override
    public void nodeDeleted(String path) {
      // TODO: do not need this callback?
      if (path.startsWith(aliveClientParentPath)) {
        String deleteNodeName = ZKUtil.getNodeName(path);
        LOG.warn("clientName=" + clientNameStr + " detect another client exit, exitClientName="
            + deleteNodeName);
        if (deleteNodeName.equals(clientNameStr)) {
          LOG.warn("the client itself exit, clientNameStr=" + clientNameStr);
          setAborted(new IOException("client zk node deleted, clientNameStr=" + clientNameStr));
        } else {
          aliveClients.remove(deleteNodeName);
        }
      }
    }

    @Override
    public void nodeChildrenChanged(String path) {
      if (path.equals(aliveClientParentPath)) {
        try {
          loadAliveClients();
        } catch (Exception e) {
          setAborted(e);
        }
      }
    }
  }
  
  class TimeoutOrDeletedHandler implements Abortable {
    private boolean aborted = false;
    
    @Override
    public void abort(String why, Throwable e) {
      LOG.error("ZookeeperWorkerRegister aborted because: " + why, e);
      setAborted(e);
    }

    @Override
    public boolean isAborted() {
      return aborted;
    }
    
  }

  private Set<String> aliveClients = new HashSet<String>(); // TODO : thread-safe?
  private final ClientTracker clientTracker;
  private final String clientNameStr;
  private final String aliveClientParentPath;
  private final String aliveClientPath;
  private final ZooKeeperWatcher watcher;
  private Throwable exception = null;
  private final String clusterName;
  
  public ZookeeperWorkerRegister(Configuration conf) throws IOException {
    super(conf);
    try {
      clientNameStr = new ClientNameWithProcessId().toString();
      clusterName = conf.get("hbase.cluster.name");
      aliveClientParentPath = getAliveClientParentPath();
      aliveClientPath = getAliveClientPath();
      watcher = new ZooKeeperWatcher(conf, clientNameStr, new TimeoutOrDeletedHandler(),
          false);
      clientTracker = new ClientTracker(watcher);
    } catch (Exception e) {
      LOG.error("init ZookeeperWorkerRegister fail", e);
      throw new IOException(e);
    }
  }
  
  protected synchronized void setAborted(Throwable e) {
    this.exception = e;
  }
  
  protected Throwable getException() {
    return this.exception;
  }
  
  protected Set<String> getAliveClients() {
    return aliveClients;
  }

  protected String getThemisClusterPath() {
    if (clusterName == null || clusterName.equals("")) {
      return THEMIS_ROOT_NODE;
    } else {
      return THEMIS_ROOT_NODE + "/" + clusterName;
    }
  }

  protected String getAliveClientParentPath() {
    return getThemisClusterPath() + "/" + ALIVE_CLIENT_NODE;
  }
  
  protected String getAliveClientPath() {
    return getAliveClientParentPath() + "/" + clientNameStr;
  }
  
  protected void createAliveParentNode() throws Exception {
    ZKUtil.createAndFailSilent(watcher, THEMIS_ROOT_NODE);
    ZKUtil.createAndFailSilent(watcher, getThemisClusterPath());
    ZKUtil.createAndFailSilent(watcher, aliveClientParentPath);
  }
  
  private void createMyEphemeralNode() throws Exception {
    createAliveParentNode();
    ZKUtil.createEphemeralNodeAndWatch(this.watcher, aliveClientPath, Bytes.toBytes(clientNameStr));
  }
  
  @Override
  public void doRegisterWorker() throws IOException {
    try {
      createMyEphemeralNode();
      clientTracker.start();
    } catch (Exception e) {
      LOG.error("register fail, clientName=" + clientNameStr, e);
      // TODO : disable this function when error happened?
      throw new IOException(e);
    }
  }

  @Override
  public boolean isWorkerAlive(String clientAddress) throws IOException {
    if (exception != null) {
      throw new IOException(exception);
    }
    return aliveClients.contains(clientAddress);
  }

  @Override
  public String getClientAddress() {
    return clientNameStr;
  }

  @Override
  public void close() throws IOException {
    if (watcher != null) {
      watcher.close();
    }
  }
}