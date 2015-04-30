package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.themis.columns.ColumnCoordinate;
import org.apache.hadoop.hbase.themis.cp.TransactionTestBase;
import org.junit.Assert;
import org.junit.Test;

// this test could only be run under 0.94
public class TestThemisExpiredDataCleanFilter extends TransactionTestBase {
  // not implemented in 0.98
}
