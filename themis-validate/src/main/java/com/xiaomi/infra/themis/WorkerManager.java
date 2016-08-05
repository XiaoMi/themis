package com.xiaomi.infra.themis;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.lockcleaner.WorkerRegister;

import com.xiaomi.infra.themis.validator.WriteWorker;


public class WorkerManager extends WorkerRegister {
  private static final Log LOG = LogFactory.getLog(WorkerManager.class);
  private static final Map<Long, WriteWorker> activeWriters = new ConcurrentHashMap<Long, WriteWorker>();
  
  public WorkerManager(Configuration conf) {
    super(conf);
  }

  @Override
  public void doRegisterWorker() throws IOException {
  }
  
  // TODO : move the logic to doRegisterWorker
  public static void registerWorker(WriteWorker worker) {
    activeWriters.put(worker.getId(), worker);
    LOG.warn("register writer name=" + worker.getWokerName() + ", id=" + worker.getId()
        + ", ActiveWorkerCount=" + activeWriters.size());
  }

  @Override
  public boolean isWorkerAlive(String clientAddress) throws IOException {
    return activeWriters.containsKey(Long.valueOf(clientAddress));
  }
  
  public static void unregisterWorker(WriteWorker worker) {
    long id = worker.getId();
    if (activeWriters.remove(id) == null) {
      LOG.fatal("worker id=" + id + ", name=" + worker.getWokerName()
          + " has been removed");
      System.exit(-1);
    }
    LOG.warn("Active worker count after unregister worker : " + activeWriters.size());
  }
  
  @Override
  public String getClientAddress() {
    String address = String.valueOf(Thread.currentThread().getId());
    return address;
  }
}