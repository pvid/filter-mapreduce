package dev.vidlicka.hbase.filtermapreduce.filters;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;

public abstract class ExtendedFilterBase extends FilterBase {
  @Override
  public ReturnCode filterKeyValue(Cell v) throws IOException {
    return ReturnCode.INCLUDE;
  }
}
