package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.themis.columns.Column;
import org.apache.hadoop.hbase.themis.columns.ColumnUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ThemisExpiredDataCleanScanner implements InternalScanner {

  private static final Logger LOG = LoggerFactory.getLogger(ThemisExpiredDataCleanScanner.class);

  private final long cleanTs;

  private final InternalScanner scanner;

  private final Optional<Region> region;

  private final List<Cell> originResult = new ArrayList<>();

  private byte[] lastRow;

  private byte[] lastFamily;

  private byte[] lastQualifier;

  public ThemisExpiredDataCleanScanner(long cleanTs, InternalScanner scanner) {
    this.cleanTs = cleanTs;
    this.scanner = scanner;
    this.region = Optional.empty();
  }

  public ThemisExpiredDataCleanScanner(long cleanTs, InternalScanner scanner, Region region) {
    this.cleanTs = cleanTs;
    this.scanner = scanner;
    this.region = Optional.of(region);
  }

  private boolean isSameColumn(Cell cell) {
    if (lastRow == null) {
      return false;
    }
    return CellUtil.matchingRows(cell, lastRow) &&
      CellUtil.matchingColumn(cell, lastFamily, lastQualifier);
  }

  private void recordLastCell(Cell cell) {
    lastRow = CellUtil.cloneRow(cell);
    lastFamily = CellUtil.cloneFamily(cell);
    lastQualifier = CellUtil.cloneQualifier(cell);
  }

  private void deleteExpiredData(Region region, Cell cell) {
    if (!ColumnUtil.isDeleteColumn(lastFamily, lastQualifier)) {
      return;
    }
    LOG.debug("First expired cell, cell=" + cell + ", cleanTs=" + cleanTs);
    Delete delete = new Delete(lastRow);
    delete.addColumns(lastFamily, lastQualifier, cell.getTimestamp());
    Column dataColumn = ColumnUtil.getDataColumn(new Column(lastFamily, lastQualifier));
    delete.addColumns(dataColumn.getFamily(), dataColumn.getQualifier(), cell.getTimestamp());
    Column putColumn = ColumnUtil.getPutColumn(dataColumn);
    delete.addColumns(putColumn.getFamily(), putColumn.getQualifier(), cell.getTimestamp());
    delete.setDurability(Durability.SKIP_WAL);
    try {
      region.delete(delete);
    } catch (IOException e) {
      LOG.error("Delete expired data fail", e);
    }
  }

  private boolean include(Cell cell) {
    if (cell.getTimestamp() >= cleanTs) {
      return true;
    }
    if (isSameColumn(cell)) {
      LOG.debug("SkipColumn cell=" + cell + ", cleanTs=" + cleanTs);
      return false;
    }
    recordLastCell(cell);
    region.ifPresent(r -> deleteExpiredData(r, cell));
    return true;
  }

  @Override
  public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
    boolean hasMore = scanner.next(originResult, scannerContext);
    originResult.stream().filter(this::include).forEach(result::add);
    originResult.clear();
    return hasMore;
  }

  @Override
  public void close() throws IOException {
    scanner.close();
  }
}
