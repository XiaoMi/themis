package org.apache.hadoop.hbase.themis.lockcleaner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.themis.ClientTestBase;
import org.junit.Test;

public class TestWorkerRegister extends ClientTestBase {

  @Test
  public void testCreateSingletonRegister() throws IOException {
    WorkerRegister register = WorkerRegister.getWorkerRegister(conf);
    assertSame(register, WorkerRegister.getWorkerRegister(conf));
  }

  @Test
  public void testOnlyRegisterOnce() throws IOException {
    WorkerRegister register = WorkerRegister.getWorkerRegister(conf);
    register.setUnregistered();
    assertTrue(register.registerWorker());
    assertFalse(register.registerWorker());
  }
}
