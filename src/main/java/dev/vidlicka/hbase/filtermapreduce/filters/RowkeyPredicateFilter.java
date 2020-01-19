package dev.vidlicka.hbase.filtermapreduce.filters;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import dev.vidlicka.hbase.filtermapreduce.SerdeUtil;

public class RowkeyPredicateFilter extends FilterBase {

  SerializablePredicate<byte[]> predicate;

  public RowkeyPredicateFilter(SerializablePredicate<byte[]> predicate) {
    this.predicate = predicate;
  }

  @Override
  public boolean filterRowKey(Cell cell) {
    // we want to filter row out, when predicate returns false
    return !predicate.test(Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(),
        cell.getRowOffset() + cell.getRowLength()));
  }

  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return SerdeUtil.serialize(predicate);
  }

  public static Filter parseFrom(byte[] bytes) throws DeserializationException {
    try {
      return new RowkeyPredicateFilter(SerdeUtil.deserialize(bytes));
    } catch (IOException | ClassNotFoundException e) {
      throw new DeserializationException(e);
    }
  }
}
