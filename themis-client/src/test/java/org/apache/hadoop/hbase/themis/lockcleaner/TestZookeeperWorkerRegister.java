package org.apache.hadoop.hbase.themis.lockcleaner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.themis.cp.ThemisEndpointClient;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.After;
import org.junit.Test;

public class TestZookeeperWorkerRegister extends ClientTestBase {
  private ZKWatcher zkw;
  protected ThemisEndpointClient cpClient;
  protected LockCleaner lockCleaner;
  
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    TestLockCleaner.setConfigForLockCleaner(conf);
    cpClient = new ThemisEndpointClient(connection);
    lockCleaner = new LockCleaner(conf, connection, mockRegister, cpClient);
    zkw = new ZKWatcher(conf, "deleteNode", null);
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
    assertTrue(ZKUtil.checkExists(zkw, ZookeeperWorkerRegister.THEMIS_ROOT_NODE) != -1);
    assertTrue(ZKUtil.checkExists(zkw, reg.getThemisClusterPath()) != -1);
    assertTrue(ZKUtil.checkExists(zkw, reg.getAliveClientParentPath()) != -1);
    assertTrue(ZKUtil.checkExists(zkw, reg.getAliveClientPath()) != -1);
  }
  
  protected ZookeeperWorkerRegister createRegisterAndStart(Configuration conf) throws IOException {
    ZookeeperWorkerRegister regA = new ZookeeperWorkerRegister(conf);
    regA.doRegisterWorker();
    return regA;
  }
  
  @Test
  public void testInitZkNodes() throws Exception {
    // firstly, create base nodes
    ZookeeperWorkerRegister reg = createRegisterAndStart(conf);
    checkAliveClientPatch(reg);
    reg.close();
    
    // create client node
    reg = createRegisterAndStart(conf);
    checkAliveClientPatch(reg);
    assertEquals(1, reg.getAliveClients().size());
    assertTrue(reg.isWorkerAlive(reg.getClientAddress()));
    reg.close();
  }
  
  @Test
  public void testClientNodeChange() throws Exception {
    ZookeeperWorkerRegister regA = createRegisterAndStart(conf);
    
    // check node add
    ZookeeperWorkerRegister regB = createRegisterAndStart(conf);
    assertEquals(2, regB.getAliveClients().size());
    Threads.sleep(100);
    assertEquals(2, regA.getAliveClients().size());
    assertTrue(regA.getAliveClients().contains(regA.getClientAddress()));
    assertTrue(regA.getAliveClients().contains(regB.getClientAddress()));
    assertTrue(regB.getAliveClients().contains(regA.getClientAddress()));
    assertTrue(regB.getAliveClients().contains(regB.getClientAddress()));
    
    // check child node lose lock
    regA.close();
    Threads.sleep(100);
    assertEquals(1, regB.getAliveClients().size());
    assertTrue(regB.getAliveClients().contains(regB.getClientAddress()));
    
    // check child node deleted
    ZookeeperWorkerRegister regC = createRegisterAndStart(conf);
    Threads.sleep(100);
    assertEquals(2, regB.getAliveClients().size());
    ZKUtil.deleteNode(zkw, regC.getAliveClientPath());
    Threads.sleep(100);
    assertEquals(1, regB.getAliveClients().size());
    assertTrue(regB.isWorkerAlive(regB.getClientAddress()));
    
    try {
      assertTrue(regC.isWorkerAlive(regB.getClientAddress()));
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("client zk node deleted") >= 0);
    }
    
    regC.close();
    regB.close();
  }
  
  @Test
  public void testShouldCleanLockWithZkRegister() throws Exception {
    ZookeeperWorkerRegister regA = createRegisterAndStart(conf);
    ZookeeperWorkerRegister regB = createRegisterAndStart(conf);
    
    lockCleaner = new LockCleaner(conf, connection, regB, cpClient);
    ThemisLock lock = getLock(COLUMN);
    lock.setClientAddress(regA.getClientAddress());
    assertFalse(lockCleaner.shouldCleanLock(lock));
    regA.close();
    Threads.sleep(100);
    assertTrue(lockCleaner.shouldCleanLock(lock));
    
    ZookeeperWorkerRegister regC = createRegisterAndStart(conf);
    lock.setClientAddress(regC.getClientAddress());
    assertFalse(lockCleaner.shouldCleanLock(lock));
    ZKUtil.deleteNode(zkw, regC.getAliveClientPath());
    Threads.sleep(100);
    assertTrue(lockCleaner.shouldCleanLock(lock));
    
    regC.close();
    regB.close();
  }
  
  protected ThemisLock prewritePrimaryRow(ZookeeperWorkerRegister reg) throws IOException {
    ThemisLock lock = getLock(COLUMN);
    lock.setClientAddress(reg.getClientAddress());
    byte[] primaryLock = ThemisLock.toByte(lock);
    lock = getLock(COLUMN_WITH_ANOTHER_TABLE);
    lock.setClientAddress(reg.getClientAddress());
    byte[] secondaryLock = ThemisLock.toByte(lock);
    ThemisLock conflictLock = cpClient.prewriteRow(COLUMN.getTableName(), PRIMARY_ROW.getRow(),
      PRIMARY_ROW.mutationList(), prewriteTs, primaryLock, secondaryLock, 2);
    return conflictLock;
  }
  
  @Test
  public void testLockCleanWithZkRegister() throws Exception {
    ZookeeperWorkerRegister regA = createRegisterAndStart(conf);
    createTransactionWithWorkRegister(regA);
    prewritePrimaryRow(regA);
    long conflictTs = prewriteTs;

    Threads.sleep(10);
    nextTransactionTs();
    ZookeeperWorkerRegister regB = createRegisterAndStart(conf);
    createTransactionWithWorkRegister(regB);
    preparePrewriteWithoutCreateTransaction(false);
    try {
      transaction.prewritePrimary();
      fail();
    } catch (IOException e) {
      assertTrue(e instanceof LockConflictException);
      assertTrue(e.getMessage().indexOf(String.valueOf(conflictTs)) >= 0);
    }
    
    // should clean lock because regA lose lock
    regA.close();
    Threads.sleep(100);
    transaction.prewritePrimary();
    ThemisLock lock = getLock(COLUMN);
    lock.setClientAddress(regB.getClientAddress());
    byte[] lockBytes = ThemisLock.toByte(lock);
    assertArrayEquals(lockBytes, readLockBytes(COLUMN, prewriteTs));
    assertArrayEquals(VALUE, readDataValue(COLUMN, prewriteTs));
    regB.close();
  }
}
