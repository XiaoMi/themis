package org.apache.hadoop.hbase.themis.lockcleaner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.themis.TransactionConstant;

public abstract class WallClock {
  private static WallClock wallClock = null;
  private static Object wallClockLock = new Object();
  
  public WallClock(Configuration conf) {}
  
  public static WallClock getWallTimer(Configuration conf) throws IOException {
    if (wallClock == null) {
      synchronized (wallClockLock) {
        if (wallClock == null) {
          try {
            wallClock = (WallClock) (Class.forName(conf.get(TransactionConstant.WALL_TIMER_CLASS_KEY,
              TransactionConstant.DEFAULT_WALL_TIMER_CLASS)).getConstructor(Configuration.class).newInstance(conf));
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      }
    }
    return wallClock;
  }
  
  public abstract long getWallTime();
  
  public static class LocalWallClock extends WallClock {
    public LocalWallClock(Configuration conf) {
      super(conf);
    }

    @Override
    public long getWallTime() {
      return System.currentTimeMillis();
    }
  }
}