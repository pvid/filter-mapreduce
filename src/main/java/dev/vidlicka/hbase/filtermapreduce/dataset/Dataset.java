package dev.vidlicka.hbase.filtermapreduce.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import dev.vidlicka.hbase.filtermapreduce.filters.CellMapperFilter;
import dev.vidlicka.hbase.filtermapreduce.filters.CellPredicateFilter;
import dev.vidlicka.hbase.filtermapreduce.filters.RowMapperFilter;
import dev.vidlicka.hbase.filtermapreduce.filters.RowkeyPredicateFilter;
import dev.vidlicka.hbase.filtermapreduce.filters.SerializableFunction;
import dev.vidlicka.hbase.filtermapreduce.filters.SerializablePredicate;
import dev.vidlicka.hbase.filtermapreduce.reducer.ReducerClient;
import dev.vidlicka.hbase.filtermapreduce.reducer.SerializableBiFunction;

/**
 * Wrapper class that provides filtermapreduce™ functionality over a regular HBase scan
 */
public class Dataset {

  private Table table;
  private Scan scan;

  private List<Filter> filterTransformations;

  private Dataset(Table table, Scan scan, List<Filter> filterTransformations) {
    this.table = table;
    this.scan = scan;
    this.filterTransformations = filterTransformations;
  }

  /**
   * Create a Dataset object for a given HBase table and scan
   * 
   * @param table HBase table containing the dataset
   * @param scan Initial scan selecting a subset of the table
   */
  public Dataset(Table table, Scan scan) {
    this(table, scan, new ArrayList<>());
  }

  /**
   * Create a Dataset object for a full table scan of an HBase table
   * 
   * @param table HBase table containing the dataset
   */
  public Dataset(Table table) {
    this(table, new Scan());
  }

  private Scan deepCopy(Scan scan) throws IOException {
    ClientProtos.Scan protoScan = ProtobufUtil.toScan(scan);
    return ProtobufUtil.toScan(protoScan);
  }

  private List<Filter> copyAndAdd(List<Filter> list, Filter element) {
    List<Filter> copied = new ArrayList<>(list);
    copied.add(element);
    return copied;
  }

  /**
   * Creates a new Dataset by mapping over cells
   * 
   * @param func Function used to map over cells
   * @return Dataset with mapped cells
   * @throws IOException
   */
  public Dataset mapCells(SerializableFunction<Cell, Cell> func) throws IOException {
    return new Dataset(table, deepCopy(scan),
        copyAndAdd(filterTransformations, new CellMapperFilter(func)));
  }

  /**
   * Creates a new Dataset by mapping over cell values
   * 
   * @param func Function used to map over cell values
   * @return Dataset with mapped cell values
   * @throws IOException
   */
  public Dataset mapCellValues(SerializableFunction<byte[], byte[]> func) throws IOException {
    SerializableFunction<Cell, Cell> cellMap =
        cell -> CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
            .setFamily(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
            .setQualifier(cell.getQualifierArray(), cell.getQualifierOffset(),
                cell.getQualifierLength())
            .setTimestamp(cell.getTimestamp()).setType(cell.getType())
            // apply the function to cell value
            .setValue(func.apply(CellUtil.cloneValue(cell))).build();
    return mapCells(cellMap);
  }

  /**
   * Creates a new Dataset by mapping over rows
   * 
   * @param func Function used to map over rows
   * @return Dataset with mapped rows
   * @throws IOException
   */
  public Dataset mapRows(SerializableFunction<List<Cell>, List<Cell>> func) throws IOException {
    return new Dataset(table, deepCopy(scan),
        copyAndAdd(filterTransformations, new RowMapperFilter(func)));
  }

  /**
   * Creates a new Dataset by filtering our cells that do not satisfy the predicate
   * 
   * @param predicate Function that decides which cells to keep
   * @return Filtered Dataset
   * @throws IOException
   */
  public Dataset filterCells(SerializablePredicate<Cell> predicate) throws IOException {
    return new Dataset(table, deepCopy(scan),
        copyAndAdd(filterTransformations, new CellPredicateFilter(predicate)));
  }

  /**
   * Creates a new Dataset by filtering our rows whose rowkeys do not satisfy the predicate
   * 
   * @param predicate Function that decides which rows to keep
   * @return Filtered Dataset
   * @throws IOException
   */
  public Dataset filterByRowkey(SerializablePredicate<byte[]> predicate) throws IOException {
    return new Dataset(table, deepCopy(scan),
        copyAndAdd(filterTransformations, new RowkeyPredicateFilter(predicate)));
  }

  private Scan getFinalScan() {
    if (!filterTransformations.isEmpty()) {
      List<Filter> allFilters = new ArrayList<>(filterTransformations);
      if (scan.getFilter() != null)
        allFilters.add(0, scan.getFilter());
      scan.setFilter(new FilterList(allFilters));
      filterTransformations.clear();
    }
    return scan;
  }

  /**
   * Returns a scanner which allows an access to data contained in the Dataset
   *
   * @return Scanner providing the Dataset data
   * @throws IOException
   */
  public ResultScanner toScanner() throws IOException {
    return table.getScanner(getFinalScan());
  }

  /**
   * Reduces the dataset into a single value
   * 
   * It does that by reducing the rows in each region using the <code>initialReduceValue</code> and
   * the <code>reducer</code> function and then combining the results from each region using
   * <code>initialMergeValue</code> and function <code>merger</code> on the client side
   * 
   * @param <A> Type of the result of the reducer
   * @param <B> Type of the result of the merger
   * @param initialReduceValue initial value for the reducer
   * @param reducer function used to combine values in regions
   * @param initialMergeValue initial value for the merger
   * @param merger function used to combine partial results from regions
   * @return Reduce result
   * @throws Throwable
   */
  public <A, B> B reduceRows(A initialReduceValue, SerializableBiFunction<A, List<Cell>, A> reducer,
      B initialMergeValue, SerializableBiFunction<B, A, B> merger) throws Throwable {
    return ReducerClient.call(table, getFinalScan(), initialReduceValue, reducer, initialMergeValue,
        merger);
  }

  /**
   * Reduces the dataset into a single value
   * 
   * It does that by reducing the cells in each region using the <code>initialReduceValue</code> and
   * the <code>reducer</code> function and then combining the results from each region using
   * <code>initialMergeValue</code> and function <code>merger</code> on the client side
   * 
   * @param <A> Type of the result of the reducer
   * @param <B> Type of the result of the merger
   * @param initialReduceValue initial value for the reducer
   * @param reducer function used to combine values in regions
   * @param initialMergeValue initial value for the merger
   * @param merger function used to combine partial results from regions
   * @return Reduce result
   * @throws Throwable
   */
  public <A, B> B reduceCells(A initialReduceValue, SerializableBiFunction<A, Cell, A> reducer,
      B initialMergeValue, SerializableBiFunction<B, A, B> merger) throws Throwable {
    return this.reduceRows(initialReduceValue, ReducerClient.createRowReducer(reducer),
        initialMergeValue, merger);
  }

  /**
   * Reduces the dataset into a single value
   * 
   * It does that by reducing the cell values in each region using the
   * <code>initialReduceValue</code> and the <code>reducer</code> function and then combining the
   * results from each region using <code>initialMergeValue</code> and function <code>merger</code>
   * on the client side
   * 
   * @param <A> Type of the result of the reducer
   * @param <B> Type of the result of the merger
   * @param initialReduceValue initial value for the reducer
   * @param reducer function used to combine values in regions
   * @param initialMergeValue initial value for the merger
   * @param merger function used to combine partial results from regions
   * @return Reduce result
   * @throws Throwable
   */
  public <A, B> B reduceCellValues(A initialReduceValue,
      SerializableBiFunction<A, byte[], A> reducer, B initialMergeValue,
      SerializableBiFunction<B, A, B> merger) throws Throwable {
    SerializableBiFunction<A, Cell, A> cellReducer =
        (acc, cell) -> reducer.apply(acc, CellUtil.cloneValue(cell));
    return this.reduceRows(initialReduceValue, ReducerClient.createRowReducer(cellReducer),
        initialMergeValue, merger);
  }
}
