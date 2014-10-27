package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;

public class ClientName {
  protected String addr;
  protected String id;
  protected Long startCode;
  
  public ClientName(String id) throws IOException {
    addr = InetAddress.getLocalHost().getHostAddress().toString();
    this.id = id;
    this.startCode = System.currentTimeMillis();
  }
  
  @Override
  public String toString() {
    return addr + "," + id + "," + startCode;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + addr.hashCode();
    result = prime * result + id.hashCode();
    return prime * result + startCode.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ClientName)) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    ClientName clientName = (ClientName)obj;
    return this.addr.equals(clientName.addr) && this.id.equals(clientName.id)
        && this.startCode.equals(clientName.startCode);
  }
    
  public static class ClientNameWithProcessId extends ClientName {
    
    public static String getProcessId() {
      RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
      String name = runtime.getName();
      return name.substring(0, name.indexOf("@"));
    }
    
    public ClientNameWithProcessId() throws IOException {
      super(getProcessId());
    }
  }
  
  public static class ClientNameWithThreadId extends ClientName {
    
    public ClientNameWithThreadId() throws IOException {
      super(String.valueOf(Thread.currentThread().getId()));
    }
  }
}