package dev.vidlicka.hbase.filtermapreduce.filters;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

import dev.vidlicka.hbase.filtermapreduce.SerdeUtil;

public class CellPredicateFilter extends FilterBase {

  SerializablePredicate<Cell> predicate;

  public CellPredicateFilter(SerializablePredicate<Cell> predicate) {
    this.predicate = predicate;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) throws IOException {
    if (predicate.test(v)) {
      return ReturnCode.INCLUDE;
    }
    return ReturnCode.SKIP;
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return SerdeUtil.serialize(predicate);
  }

  public static Filter parseFrom(byte[] bytes) throws DeserializationException {
    try {
      return new CellPredicateFilter(SerdeUtil.deserialize(bytes));
    } catch (IOException | ClassNotFoundException e) {
      throw new DeserializationException(e);
    }
  }
}
