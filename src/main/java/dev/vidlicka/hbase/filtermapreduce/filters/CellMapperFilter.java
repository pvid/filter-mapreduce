package dev.vidlicka.hbase.filtermapreduce.filters;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import dev.vidlicka.hbase.filtermapreduce.SerdeUtil;

public class CellMapperFilter extends FilterBase {

  SerializableFunction<Cell, Cell> func;

  public CellMapperFilter(SerializableFunction<Cell, Cell> func) {
    this.func = func;
  }

  @Override
  public Cell transformCell(Cell v) throws IOException {
    return func.apply(v);
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return SerdeUtil.serialize(func);
  }

  public static Filter parseFrom(byte[] bytes) throws DeserializationException {
    try {
      return new CellMapperFilter(SerdeUtil.deserialize(bytes));
    } catch (IOException | ClassNotFoundException e) {
      throw new DeserializationException(e);
    }
  }
}
