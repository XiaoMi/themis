package com.xiaomi.infra.themis.exception;

import java.io.IOException;

public class GeneratedTestException extends IOException {
  private static final long serialVersionUID = -5941841630837922183L;

  public GeneratedTestException(String msg) {
    super("Generated Exception : " + msg);
  }
}
