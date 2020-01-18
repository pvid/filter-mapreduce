package dev.vidlicka.hbase.filtermapreduce.test;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class TestUtils {

  public static byte[] QUALIFIER = Bytes.toBytes("qualifier");
  public static byte[] CF = Bytes.toBytes("test_cf");

  public static ArrayList<Result> extractAllResults(ResultScanner scanner) throws IOException {
    ArrayList<Result> results = new ArrayList<>();
    for (Result result = scanner.next(); result != null; result = scanner.next()) {
      results.add(result);
    }
    return results;
  }

  public static Put putLongValue(Long rowkey, byte[] cf, byte[] qualifier, Long value) {
    Put put = new Put(Bytes.toBytes(rowkey));
    put.addColumn(cf, qualifier, Bytes.toBytes(value));
    return put;
  }

  public static Put putLongValue(Long rowkey, Long value) {
    return putLongValue(rowkey, CF, QUALIFIER, value);
  }
}
