package org.apache.hadoop.hbase.themis.cp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.util.Pair;

// Currently, hbase could not set timestamp for each column in Get/Scan. ColumnTimestampFilter
// is implemented to specify the timestamp for each column. 
public class ColumnTimestampFilter extends FilterBase {
  // index timestamp for each column
  private List<Pair<Column, Long>> columnsTs = new ArrayList<>();
  private int curColumnIdx = -1;
  
  // TODO(cuijianwei) : should check add deplicated column?
  public void addColumnTimestamp(Column column, long timestamp) {
    columnsTs.add(new Pair<>(new Column(column.getFamily(), column
            .getQualifier()), timestamp));
  }

  private void sortColumnsTs() {
    columnsTs.sort(Comparator.comparing(Pair::getFirst));
  }
  
  public ReturnCode filterKeyValue(KeyValue v) {
    if (curColumnIdx == -1) {
      sortColumnsTs();
      curColumnIdx = 0;
    }
    
    if (curColumnIdx >= columnsTs.size()) {
      return ReturnCode.NEXT_ROW;
    }
    
    Column column = new Column(v.getFamilyArray(), v.getQualifierArray());
    Column curColumn;
    Long curTs;
    int cmpRet;
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
  public Cell getNextCellHint(Cell kv) {
    if (curColumnIdx >= columnsTs.size()) {
      return null;
    }

    Column column = columnsTs.get(curColumnIdx).getFirst();
    return KeyValueUtil.createFirstOnRow(kv.getRowArray(), column.getFamily(), column.getQualifier());
  }
  
  public void readFields(DataInput arg0) throws IOException {
    throw new IOException("not implemented");
  }

  public void write(DataOutput arg0) throws IOException {
    throw new IOException("not implemented");
  }
}