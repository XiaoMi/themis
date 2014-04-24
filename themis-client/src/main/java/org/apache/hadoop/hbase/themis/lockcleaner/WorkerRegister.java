package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.themis.TransactionConstant;

// As mentioned in google's themis paper, each client could register as a worker in
// Zookeeper. Then, if the client died, all unfinished transaction by the died client could
// be cleaned immediately when discovered by other clients
public abstract class WorkerRegister extends Configured {
  private static final Log LOG = LogFactory.getLog(WorkerRegister.class);
  private static WorkerRegister register;
  private static Object registerLock = new Object();
  private AtomicBoolean registered = new AtomicBoolean(false);
  
  public static WorkerRegister getWorkerRegister(Configuration conf) throws IOException {
    String workerRegisterCls = conf.get(TransactionConstant.WORKER_REGISTER_CLASS_KEY,
      TransactionConstant.DEFAULT_WORKER_REISTER_CLASS);
    if (register == null) {
      synchronized (registerLock) {
        if (register == null) {
          try {
            register = (WorkerRegister) (Class.forName(workerRegisterCls).getConstructor(Configuration.class)
                .newInstance(conf));
          } catch (Exception e) {
            LOG.fatal("create workerRegister fail", e);
            throw new IOException(e);
          }          
        }
      }
    }
    return register;
  }
  
  public WorkerRegister(Configuration conf) {
    super(conf);
  }
  
  public boolean registerWorker() throws IOException {
    // only register when the worker is not registered
    if (registered.compareAndSet(false, true)) {
      doRegisterWorker();
      return true;
    }
    return false;
  }
  
  protected void setUnregistered() throws IOException {
    registered.compareAndSet(true, false);
  }

  public abstract void doRegisterWorker() throws IOException;
    
  public abstract boolean isWorkerAlive(String clientAddress) throws IOException;

  public abstract String getClientAddress();
  
  public static class NullWorkerRegister extends WorkerRegister {
    private static final Log LOG = LogFactory.getLog(NullWorkerRegister.class);

    public NullWorkerRegister(Configuration conf) {
      super(conf);
    }

    // TODO(cuijianwei): implement a ZookeeperWorkerRegister to register client to register
    //                   alive clients into zookeeper to help discover died client and lock clean
    @Override
    public void doRegisterWorker() throws IOException {
      LOG.info("do nothing in registerWorker");
    }

    @Override
    public boolean isWorkerAlive(String clientAddress) throws IOException {
      return true;
    }

    @Override
    public String getClientAddress() {
      return "null-client-address";
    }
  }
}
