package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.themis.cp.ThemisCoprocessorClient;
import org.apache.hadoop.hbase.themis.exception.LockConflictException;
import org.apache.hadoop.hbase.themis.lock.ThemisLock;
import org.apache.hadoop.hbase.themis.lockcleaner.ClientName.ClientNameWithProcessId;
import org.apache.hadoop.hbase.themis.lockcleaner.ZookeeperWorkerRegister.ZookeeperWorkerRegisterByThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestZookeeperWorkerRegister extends ClientTestBase {
  private ZKWatcher zkw;
  protected ThemisCoprocessorClient cpClient;
  protected LockCleaner lockCleaner;
  
  @Override
  public void initEnv() throws IOException {
    super.initEnv();
    TestLockCleaner.setConfigForLockCleaner(conf);
    cpClient = new ThemisCoprocessorClient(connection);
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
    Assert.assertTrue(ZKUtil.checkExists(zkw, ZookeeperWorkerRegister.THEMIS_ROOT_NODE) != -1);
    Assert.assertTrue(ZKUtil.checkExists(zkw, reg.getThemisClusterPath()) != -1);
    Assert.assertTrue(ZKUtil.checkExists(zkw, reg.getAliveClientParentPath()) != -1);
    Assert.assertTrue(ZKUtil.checkExists(zkw, reg.getAliveClientPath()) != -1);
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
    Assert.assertEquals(1, reg.getAliveClients().size());
    Assert.assertTrue(reg.isWorkerAlive(reg.getClientAddress()));
    reg.close();
  }
  
  @Test
  public void testClientNodeChange() throws Exception {
    ZookeeperWorkerRegister regA = createRegisterAndStart(conf);
    
    // check node add
    ZookeeperWorkerRegister regB = createRegisterAndStart(conf);
    Assert.assertEquals(2, regB.getAliveClients().size());
    Threads.sleep(100);
    Assert.assertEquals(2, regA.getAliveClients().size());
    Assert.assertTrue(regA.getAliveClients().contains(regA.getClientAddress()));
    Assert.assertTrue(regA.getAliveClients().contains(regB.getClientAddress()));
    Assert.assertTrue(regB.getAliveClients().contains(regA.getClientAddress()));
    Assert.assertTrue(regB.getAliveClients().contains(regB.getClientAddress()));
    
    // check child node lose lock
    regA.close();
    Threads.sleep(100);
    Assert.assertEquals(1, regB.getAliveClients().size());
    Assert.assertTrue(regB.getAliveClients().contains(regB.getClientAddress()));
    
    // check child node deleted
    ZookeeperWorkerRegister regC = createRegisterAndStart(conf);
    Threads.sleep(100);
    Assert.assertEquals(2, regB.getAliveClients().size());
    ZKUtil.deleteNode(zkw, regC.getAliveClientPath());
    Threads.sleep(100);
    Assert.assertEquals(1, regB.getAliveClients().size());
    Assert.assertTrue(regB.isWorkerAlive(regB.getClientAddress()));
    
    try {
      Assert.assertTrue(regC.isWorkerAlive(regB.getClientAddress()));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().indexOf("client zk node deleted") >= 0);
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
    Assert.assertFalse(lockCleaner.shouldCleanLock(lock));
    regA.close();
    Threads.sleep(100);
    Assert.assertTrue(lockCleaner.shouldCleanLock(lock));
    
    ZookeeperWorkerRegister regC = createRegisterAndStart(conf);
    lock.setClientAddress(regC.getClientAddress());
    Assert.assertFalse(lockCleaner.shouldCleanLock(lock));
    ZKUtil.deleteNode(zkw, regC.getAliveClientPath());
    Threads.sleep(100);
    Assert.assertTrue(lockCleaner.shouldCleanLock(lock));
    
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
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e instanceof LockConflictException);
      Assert.assertTrue(e.getMessage().indexOf(String.valueOf(conflictTs)) >= 0);
    }
    
    // should clean lock because regA lose lock
    regA.close();
    Threads.sleep(100);
    transaction.prewritePrimary();
    ThemisLock lock = getLock(COLUMN);
    lock.setClientAddress(regB.getClientAddress());
    byte[] lockBytes = ThemisLock.toByte(lock);
    Assert.assertArrayEquals(lockBytes, readLockBytes(COLUMN, prewriteTs));
    Assert.assertArrayEquals(VALUE, readDataValue(COLUMN, prewriteTs));
    regB.close();
  }
  
  @Test
  public void TestZookeeperWorkerRegisterByThread() throws IOException {
    ZookeeperWorkerRegisterByThread register = new ZookeeperWorkerRegisterByThread(conf);
    Assert.assertEquals(String.valueOf(Thread.currentThread().getId()), register.getClientAddress().split(",")[1]);
    Assert.assertFalse(register.getClientAddress().split(",")[1].equals(new ClientNameWithProcessId().id));
    register.close();
  }
}
