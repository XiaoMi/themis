package org.apache.hadoop.hbase.themis.cp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.util.Pair;

// Currently, hbase could not set timestamp for each column in Get/Scan. ColumnTimestampFilter
// is implemented to specify the timestamp for each column. 
public class ColumnTimestampFilter extends FilterBase {
  // index timestamp for each column
  private List<Pair<Column, Long>> columnsTs = new ArrayList<Pair<Column, Long>>();
  private int curColumnIdx = -1;
  
  // TODO(cuijianwei) : should check add deplicated column?
  public void addColumnTimestamp(Column column, long timestamp) {
    columnsTs.add(new Pair<Column, Long>(new Column(column.getFamily(), column
        .getQualifier()), timestamp));
  }

  private void sortColumnsTs() {
    Collections.sort(columnsTs, new Comparator<Pair<Column, Long>>() {
      public int compare(Pair<Column, Long> o1, Pair<Column, Long> o2) {
        return o1.getFirst().compareTo(o2.getFirst());
      }
    });
  }
  
  public ReturnCode filterKeyValue(KeyValue v) {
    if (curColumnIdx == -1) {
      sortColumnsTs();
      curColumnIdx = 0;
    }
    
    if (curColumnIdx >= columnsTs.size()) {
      return ReturnCode.NEXT_ROW;
    }
    
    Column column = new Column(v.getFamily(), v.getQualifier());
    Column curColumn = null;
    Long curTs = null;
    int cmpRet = 0;
    do {
      curColumn = columnsTs.get(curColumnIdx).getFirst();
      curTs = columnsTs.get(curColumnIdx).getSecond();
    } while ((cmpRet = curColumn.compareTo(column)) < 0 && ++curColumnIdx < columnsTs.size());
    
    if (cmpRet < 0) {
      return ReturnCode.NEXT_ROW;
    } else if (cmpRet > 0){
      return ReturnCode.SEEK_NEXT_USING_HINT;
    } else {
      if (curTs.equals(v.getTimestamp())) {
        ++curColumnIdx;
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      } else if (curTs > v.getTimestamp()) {
        return ReturnCode.NEXT_COL;      
      } else {
        return ReturnCode.SKIP;
      }
    }
  }

  @Override
  public void reset() {
    curColumnIdx = 0;
  }
  
  @Override
  public KeyValue getNextKeyHint(KeyValue kv) {
    if (curColumnIdx >= columnsTs.size()) {
      return null;
    }
    
    Column column = columnsTs.get(curColumnIdx).getFirst();
    return KeyValue.createFirstOnRow(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(),
      column.getFamily(), 0, column.getFamily() == null ? 0 : column.getFamily().length,
      column.getQualifier(), 0, column.getQualifier() == null ? 0 : column.getQualifier().length);
  }
  
  public void readFields(DataInput arg0) throws IOException {
    throw new IOException("not implemented");
  }

  public void write(DataOutput arg0) throws IOException {
    throw new IOException("not implemented");
  }
}