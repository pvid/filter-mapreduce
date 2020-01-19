package dev.vidlicka.hbase.filtermapreduce.dataset;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import dev.vidlicka.hbase.filtermapreduce.MiniClusterSuite;
import dev.vidlicka.hbase.filtermapreduce.filters.RowkeyPredicateFilter;
import dev.vidlicka.hbase.filtermapreduce.reducer.ReducerEndpoint;
import dev.vidlicka.hbase.filtermapreduce.test.TestUtils;

public class BasicDatasetSuiteElement {
  private static byte[] TABLE = Bytes.toBytes("dataset_test_table");

  private static Long ZERO = 0L;

  private static Table table;

  @BeforeClass
  public static void setup() throws IOException {
    // create table
    TableDescriptor mapreduceTableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(TestUtils.CF))
        .setCoprocessor(ReducerEndpoint.class.getName()).build();
    MiniClusterSuite.hbase.getAdmin().createTable(mapreduceTableDesc);

    // put in some values
    ArrayList<Put> puts = new ArrayList<>();

    for (long i = 0; i < 50; i++) {
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
  public void datasetCountRows() throws Throwable {
    Dataset dataset = new Dataset(table);

    Long result =
        dataset.reduceRows(ZERO, (acc, row) -> acc + 1, ZERO, (acc, count) -> acc + count);
    assertEquals((long) 50L, (long) result);
  }

  @Test
  public void datasetFromScan() throws Throwable {
    Scan scan = new Scan().withStartRow(Bytes.toBytes(20L)).withStopRow(Bytes.toBytes(30L));

    Dataset dataset = new Dataset(table, scan);

    Long result =
        dataset.reduceRows(ZERO, (acc, row) -> acc + 1, ZERO, (acc, count) -> acc + count);
    assertEquals((long) 10L, (long) result);
  }

  @Test
  public void rowkeyFilter() throws Throwable {
    Dataset dataset = new Dataset(table);

    Dataset filtered = dataset.filterByRowkey(rowkey -> Bytes.toLong(rowkey) < 5L);

    Long result =
        filtered.reduceRows(ZERO, (acc, row) -> acc + 1, ZERO, (acc, count) -> acc + count);
    assertEquals((long) 5L, (long) result);
  }

  @Test
  public void cellFilter() throws Throwable {
    Dataset dataset = new Dataset(table);

    Dataset filtered = dataset.filterCells(cell -> Bytes.toLong(CellUtil.cloneValue(cell)) < 5L);

    Long result =
        filtered.reduceRows(ZERO, (acc, row) -> acc + 1, ZERO, (acc, count) -> acc + count);
    assertEquals((long) 5L, (long) result);
  }

  @Test
  public void combileFilterAndScanFilter() throws Throwable {
    Scan scan = new Scan();
    scan.setFilter(new RowkeyPredicateFilter(rowkey -> Bytes.toLong(rowkey) > 0L));

    Dataset dataset = new Dataset(table, scan);

    Dataset filtered = dataset.filterByRowkey(rowkey -> Bytes.toLong(rowkey) < 5L);

    Long result =
        filtered.reduceRows(ZERO, (acc, row) -> acc + 1, ZERO, (acc, count) -> acc + count);
    assertEquals((long) 4L, (long) result);
  }

  @Test
  public void getScanner() throws Throwable {
    Scan scan = new Scan();
    scan.setFilter(new RowkeyPredicateFilter(rowkey -> Bytes.toLong(rowkey) > 0L));

    Dataset dataset = new Dataset(table, scan);

    Dataset filtered = dataset.filterByRowkey(rowkey -> Bytes.toLong(rowkey) < 5L);

    List<Result> results = TestUtils.extractAllResults(filtered.toScanner());
    assertEquals(4, results.size());
  }

  @Test
  public void operationChaining() throws Throwable {
    Scan scan = new Scan().withStartRow(Bytes.toBytes(20L)).withStopRow(Bytes.toBytes(40L));

    Dataset dataset = new Dataset(table, scan);

    Long result = dataset.filterByRowkey(rowkey -> Bytes.toLong(rowkey) < 35L)
        .mapCellValues(bytes -> Bytes.toBytes(2L)).reduceCellValues(ZERO,
            (acc, value) -> acc + Bytes.toLong(value), ZERO, (acc, count) -> acc + count);
    assertEquals((long) 30L, (long) result);
  }

  @Test
  public void branching() throws Throwable {
    Dataset base = new Dataset(table);
    Dataset firstBranch = base.filterByRowkey(rowkey -> Bytes.toLong(rowkey) < 35L);
    Dataset secondBranch = base.filterByRowkey(rowkey -> Bytes.toLong(rowkey) < 5L);

    long firstBranchRowCount =
        firstBranch.reduceRows(ZERO, (acc, row) -> acc + 1, ZERO, (acc, count) -> acc + count);
    assertEquals((long) 35, (long) firstBranchRowCount);

    long secondBranchRowCount =
        secondBranch.reduceRows(ZERO, (acc, row) -> acc + 1, ZERO, (acc, count) -> acc + count);
    assertEquals((long) 5, (long) secondBranchRowCount);

    long baseRowCount =
        base.reduceRows(ZERO, (acc, row) -> acc + 1, ZERO, (acc, count) -> acc + count);
    assertEquals((long) 50, (long) baseRowCount);
  }
}
