package org.apache.hadoop.hbase.themis.exception;

import org.apache.hadoop.hbase.themis.columns.RowMutation;

/**
 * Created by qiankai on 18/3/27.
 */
public class InvalidRowMutationException extends ThemisException {
  private static final long serialVersionUID = -5300909468331086845L;
  private RowMutation rowMutation;

  public InvalidRowMutationException(String msg, RowMutation rowMutation) {
    super(msg);
    this.rowMutation = rowMutation;
  }

  public RowMutation getRowMutation() {
    return rowMutation;
  }
}
