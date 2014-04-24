package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.apache.hadoop.hbase.themis.lockcleaner.WorkerRegister;
import org.junit.Test;

public class TestWorkerRegister extends ClientTestBase {
  @Test
  public void testCreateSingletonRegister() throws IOException {
    WorkerRegister register = WorkerRegister.getWorkerRegister(conf);
    Assert.assertSame(register, WorkerRegister.getWorkerRegister(conf));
  }
  
  @Test
  public void testOnlyRegisterOnce() throws IOException {
    WorkerRegister register = WorkerRegister.getWorkerRegister(conf);
    register.setUnregistered();
    Assert.assertTrue(register.registerWorker());
    Assert.assertFalse(register.registerWorker());
  }
}
