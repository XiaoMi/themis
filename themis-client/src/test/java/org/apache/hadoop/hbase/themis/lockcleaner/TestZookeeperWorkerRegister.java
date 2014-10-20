package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;

import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestZookeeperWorkerRegister extends ClientTestBase {
  private ZooKeeperWatcher zkw;
  
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    zkw = new ZooKeeperWatcher(conf, "deleteNode", null);
  }
  
  @After
  public void tearUp() throws IOException {
    try {
      ZKUtil.deleteNodeRecursively(zkw, ZookeeperWorkerRegister.THEMIS_ROOT_NODE);
    } catch (Exception e) {
      throw new IOException(e);
    }
    zkw.close();
    super.tearUp();
  }

  protected void checkAliveClientPatch(ZookeeperWorkerRegister reg) throws Exception {
    Assert.assertTrue(ZKUtil.checkExists(zkw, ZookeeperWorkerRegister.THEMIS_ROOT_NODE) != -1);
    Assert.assertTrue(ZKUtil.checkExists(zkw, reg.getThemisClusterPath()) != -1);
    Assert.assertTrue(ZKUtil.checkExists(zkw, reg.getAliveClientParentPath()) != -1);
    Assert.assertTrue(ZKUtil.checkExists(zkw, reg.getAliveClientPath()) != -1);
  }
  
  @Test
  public void testInitZkNodes() throws Exception {
    ZookeeperWorkerRegister reg = new ZookeeperWorkerRegister(conf);
    reg.doRegisterWorker();
    checkAliveClientPatch(reg);
    reg.close();
    
    reg = new ZookeeperWorkerRegister(conf);
    reg.doRegisterWorker();
    checkAliveClientPatch(reg);
    reg.close();
  }
}
