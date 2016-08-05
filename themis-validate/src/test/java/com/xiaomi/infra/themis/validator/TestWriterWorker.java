package com.xiaomi.infra.themis.validator;

import junit.framework.Assert;

import org.junit.Test;

import com.xiaomi.infra.themis.validator.WriteWorker;

public class TestWriterWorker {
  @Test
  public void testDeleteOrBalanceValue() {
    WriteWorker worker = new WriteWorker(null);
    Integer[] values = new Integer[]{1, 2};
    worker.deleteOrBalanceValue(values);
    Assert.assertEquals(0, values[0].intValue());
    Assert.assertEquals(3, values[1].intValue());
    worker.deleteOrBalanceValue(values);
    Assert.assertEquals(1, values[0].intValue());
    Assert.assertEquals(2, values[1].intValue());
    
    values = new Integer[]{0, 0};
    worker.deleteOrBalanceValue(values);
    Assert.assertEquals(0, values[0].intValue());
    Assert.assertEquals(0, values[1].intValue());
    
    values = new Integer[]{0, 0, 4};
    worker.deleteOrBalanceValue(values);
    Assert.assertEquals(0, values[0].intValue());
    Assert.assertEquals(2, values[1].intValue());
    Assert.assertEquals(2, values[2].intValue());
    worker.deleteOrBalanceValue(values);
    Assert.assertEquals(1, values[0].intValue());
    Assert.assertEquals(1, values[1].intValue());
    Assert.assertEquals(2, values[2].intValue());
  }
}
