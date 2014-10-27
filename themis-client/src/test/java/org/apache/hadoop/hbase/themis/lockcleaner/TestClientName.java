package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;

import org.apache.hadoop.hbase.themis.lockcleaner.ClientName.ClientNameWithProcessId;
import org.apache.hadoop.hbase.themis.lockcleaner.ClientName.ClientNameWithThreadId;
import org.junit.Assert;
import org.junit.Test;

public class TestClientName {
  
  public static class ClientNameById extends ClientName {
    public ClientNameById(String id) throws IOException {
      super(id);
    }
  }
  
  public static class ClientNameFromThread extends Thread {
    public String clientNameCls;
    public ClientName clientName;
    
    public ClientNameFromThread(String cls) {
      this.clientNameCls = cls;
    }
    
    @Override
    public void run() {
      try {
        clientName = (ClientName)(Class.forName(clientNameCls).newInstance());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Test
  public void testClientNameWithProcessId() throws Exception {
    ClientNameWithProcessId expect = new ClientNameWithProcessId();
    ClientNameWithProcessId actual = new ClientNameWithProcessId();
    Assert.assertEquals(expect.id, actual.id);
    ClientNameFromThread getter = new ClientNameFromThread(ClientNameWithProcessId.class.getName());
    getter.start();
    getter.join();
    Assert.assertEquals(expect.id, getter.clientName.id);
  }
  
  @Test
  public void testClientNameWithThreadId() throws Exception {
    ClientNameWithThreadId expect = new ClientNameWithThreadId();
    ClientNameWithThreadId actual = new ClientNameWithThreadId();
    Assert.assertEquals(expect.id, actual.id);
    ClientNameFromThread getter = new ClientNameFromThread(ClientNameWithThreadId.class.getName());
    getter.start();
    getter.join();
    Assert.assertFalse(expect.id.equals(getter.clientName.id));
  }
}
