package dev.vidlicka.hbase.filtermapreduce.reducer;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import dev.vidlicka.hbase.filtermapreduce.MiniClusterSuite;
import dev.vidlicka.hbase.filtermapreduce.filters.CellPredicateFilter;
import dev.vidlicka.hbase.filtermapreduce.test.TestUtils;

public class BasicReducerFunctionalitySuiteElement {
  private static byte[] TABLE = Bytes.toBytes("reducer_test_table");

  private static Table table;

  @BeforeClass
  public static void setup() throws IOException {
    // create table
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TABLE));
    tableDesc.addFamily(new HColumnDescriptor(TestUtils.CF));
    tableDesc.addCoprocessor(ReducerEndpoint.class.getName());

    MiniClusterSuite.hbase.getAdmin().createTable(tableDesc);

    // put in some values
    ArrayList<Put> puts = new ArrayList<>();

    for (long i = 0; i < 10; i++) {
      puts.add(TestUtils.putLongValue(i, i));
    }

    table = MiniClusterSuite.hbase.getTableByName(TABLE);
    table.put(puts);
  }

  @AfterClass
  public static void teardown() throws IOException {
    MiniClusterSuite.hbase.dropTable(TableName.valueOf(TABLE));
  }

  @Test
  public void reducerRowCountTest() throws IOException, ServiceException, Throwable {
    Scan scan = new Scan();

    SerializableBiFunction<Long, List<Cell>, Long> reducer = (acc, row) -> acc + 1;
    Long initialReduceValue = 0L;

    SerializableBiFunction<Long, Long, Long> merger = Long::sum;
    Long initialMergeValue = 0L;

    Long result =
        ReducerClient.call(table, scan, initialReduceValue, reducer, initialMergeValue, merger);
    assertEquals((long) 10L, (long) result);
  }

  @Test
  public void reducerCellSumTest() throws IOException, ServiceException, Throwable {
    Scan scan = new Scan();

    SerializableBiFunction<Long, List<Cell>, Long> reducer = ReducerClient
        .createRowReducer((acc, cell) -> acc + Bytes.toLong(CellUtil.cloneValue(cell)));

    Long initialReduceValue = 0L;

    SerializableBiFunction<Long, Long, Long> merger = Long::sum;
    Long initialMergeValue = 0L;

    Long result =
        ReducerClient.call(table, scan, initialReduceValue, reducer, initialMergeValue, merger);
    assertEquals((long) 45, (long) result);
  }

  @Test
  public void reducerCellFilterAndSumTest() throws IOException, ServiceException, Throwable {
    Scan scan = new Scan();

    Filter flt = new CellPredicateFilter(cell -> Bytes.toLong(CellUtil.cloneValue(cell)) < 7);
    scan.setFilter(flt);

    SerializableBiFunction<Long, List<Cell>, Long> reducer = ReducerClient
        .createRowReducer((acc, cell) -> acc + Bytes.toLong(CellUtil.cloneValue(cell)));

    Long initialReduceValue = 0L;

    SerializableBiFunction<Long, Long, Long> merger = Long::sum;
    Long initialMergeValue = 0L;

    Long result =
        ReducerClient.call(table, scan, initialReduceValue, reducer, initialMergeValue, merger);
    assertEquals((long) 21, (long) result);
  }
}
