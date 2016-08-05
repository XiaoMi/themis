package com.xiaomi.infra.themis.validator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.xiaomi.infra.themis.validator.Validator;

public class TestValidator {
  @Test
  public void testValidatorConfig() throws Exception {
    Validator.initConfig();
  }
  
  @Test
  public void testValidatorInit() throws IOException {
    Configuration conf = Validator.initConfig();
    Validator.initValidator(conf);
  }
  
  @Test
  public void testValidator() throws Exception {
    Validator.main(new String[]{});
  }
}
