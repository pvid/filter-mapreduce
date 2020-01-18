package dev.vidlicka.hbase.filtermapreduce.filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;

import dev.vidlicka.hbase.filtermapreduce.SerdeUtil;

public class RowMapperFilter extends ExtendedFilterBase {

  SerializableFunction<List<Cell>, List<Cell>> func;

  public RowMapperFilter(SerializableFunction<List<Cell>, List<Cell>> func) {
    this.func = func;
  }

  @Override
  public void filterRowCells(List<Cell> ignored) throws IOException {
    List<Cell> newRowCells = func.apply(new ArrayList<>(ignored));
    ignored.clear();
    ignored.addAll(newRowCells);
  }

  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return SerdeUtil.serialize(func);
  }

  public static Filter parseFrom(byte[] bytes) throws DeserializationException {
    try {
      return new RowMapperFilter(SerdeUtil.deserialize(bytes));
    } catch (IOException | ClassNotFoundException e) {
      throw new DeserializationException(e);
    }
  }
}
