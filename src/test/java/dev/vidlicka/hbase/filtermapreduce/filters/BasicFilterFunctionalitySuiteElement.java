package dev.vidlicka.hbase.filtermapreduce.filters;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import dev.vidlicka.hbase.filtermapreduce.MiniClusterSuite;
import dev.vidlicka.hbase.filtermapreduce.reducer.ReducerEndpoint;
import dev.vidlicka.hbase.filtermapreduce.test.TestUtils;

public class BasicFilterFunctionalitySuiteElement {
  private static byte[] TABLE = Bytes.toBytes("filter_test_table");

  private static Table table;

  @BeforeClass
  public static void setup() throws IOException {
    // create table
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(TestUtils.CF))
        .setCoprocessor(ReducerEndpoint.class.getName()).build();

    MiniClusterSuite.hbase.getAdmin().createTable(tableDesc);

    // put in some values
    ArrayList<Put> puts = new ArrayList<>();

    for (long i = 0; i < 10; i++) {
      puts.add(TestUtils.putLongValue(i, 10 + i));
      puts.add(TestUtils.putLongValue(i, TestUtils.CF, Bytes.toBytes("secondQualifier"), 20 + i));
    }

    table = MiniClusterSuite.hbase.getTableByName(TABLE);
    table.put(puts);
  }

  @AfterClass
  public static void teardown() throws IOException {
    MiniClusterSuite.hbase.dropTable(TableName.valueOf(TABLE));
  }

  @Test
  public void cellPredicateFilterTest() throws IOException {
    Scan scan = new Scan();
    scan.addColumn(TestUtils.CF, TestUtils.QUALIFIER);
    Filter flt = new CellPredicateFilter(cell -> Bytes.toLong(CellUtil.cloneValue(cell)) >= 17);
    scan.setFilter(flt);
    ResultScanner scanner = table.getScanner(scan);

    ArrayList<Result> results = TestUtils.extractAllResults(scanner);
    assertEquals(3, results.size());
  }

  @Test
  public void rowkeyPredicateFilterTest() throws IOException {
    Scan scan = new Scan();
    scan.addColumn(TestUtils.CF, TestUtils.QUALIFIER);
    Filter flt = new RowkeyPredicateFilter(rowkey -> Bytes.toLong(rowkey) < 5);
    scan.setFilter(flt);
    ResultScanner scanner = table.getScanner(scan);

    ArrayList<Result> results = TestUtils.extractAllResults(scanner);
    assertEquals(5, results.size());
  }

  @Test
  public void cellMapperFilterTest() throws IOException {
    Scan scan = new Scan();
    scan.addColumn(TestUtils.CF, TestUtils.QUALIFIER);
    // Filter flt = new CellMapperFilter(cell -> CellUtil.createCell(CellUtil.cloneRow(cell),
    // CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), cell.getTimestamp(),
    // cell.getTypeByte(), Bytes.toBytes(20L)));
    Filter flt = new CellMapperFilter(cell -> CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
        .setRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
        .setFamily(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
        .setQualifier(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength())
        .setTimestamp(cell.getTimestamp()).setType(cell.getType())
        // apply the function to cell value
        .setValue(Bytes.toBytes(20L)).build());
    scan.setFilter(flt);
    ResultScanner scanner = table.getScanner(scan);

    ArrayList<Result> results = TestUtils.extractAllResults(scanner);
    for (Result result : results) {
      List<Cell> cells = result.getColumnCells(TestUtils.CF, TestUtils.QUALIFIER);
      assertEquals(1, cells.size());
      assertArrayEquals(Bytes.toBytes(20L), CellUtil.cloneValue(cells.get(0)));
    }
  }

  @Test
  public void rowMapperFilterTest() throws IOException {
    Scan scan = new Scan();
    Filter flt = new RowMapperFilter(row -> {
      byte[] rowkey = CellUtil.cloneRow(row.get(0));
      Cell newCell = CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(rowkey)
          .setType(Cell.Type.Put).build();
      return Arrays.asList(newCell);
    });
    scan.setFilter(flt);
    ResultScanner scanner = table.getScanner(scan);

    ArrayList<Result> results = TestUtils.extractAllResults(scanner);
    assertEquals(10, results.size());
    for (Result result : results) {
      List<Cell> cells = result.listCells();
      assertEquals(1, cells.size());
      assertArrayEquals(new byte[0], CellUtil.cloneValue(cells.get(0)));
    }
  }

  @Test
  public void rowMapperFilterWithIdentityTest() throws IOException {
    Scan scan = new Scan();
    Filter flt = new RowMapperFilter(row -> row);
    scan.setFilter(flt);
    ResultScanner scanner = table.getScanner(scan);

    ArrayList<Result> results = TestUtils.extractAllResults(scanner);
    assertEquals(10, results.size());
    for (Result result : results) {
      List<Cell> cells = result.listCells();
      assertEquals(2, cells.size());
    }
  }
}
