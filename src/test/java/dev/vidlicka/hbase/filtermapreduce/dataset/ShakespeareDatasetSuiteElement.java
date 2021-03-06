package dev.vidlicka.hbase.filtermapreduce.dataset;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dev.vidlicka.hbase.filtermapreduce.MiniClusterSuite;
import dev.vidlicka.hbase.filtermapreduce.SerdeUtil;
import dev.vidlicka.hbase.filtermapreduce.filters.SerializableFunction;
import dev.vidlicka.hbase.filtermapreduce.reducer.ReducerEndpoint;
import dev.vidlicka.hbase.filtermapreduce.reducer.SerializableBiFunction;
import dev.vidlicka.hbase.filtermapreduce.test.TestUtils;

public class ShakespeareDatasetSuiteElement {

  private static Logger LOG = LoggerFactory.getLogger(ShakespeareDatasetSuiteElement.class);

  private static byte[] TABLE = Bytes.toBytes("shakespeare_table");
  private static Table shakespeareTable;

  private static Long ZERO = 0L;

  private static Map<String, Integer> emptyWordCount() {
    return new HashMap<>();
  }

  @BeforeClass
  public static void setup() throws IOException {
    LOG.info("Starting Shakespeare suite...");
    // create table
    TableDescriptor shakespeareTableDesc =
        TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(TestUtils.CF))
            .setCoprocessor(ReducerEndpoint.class.getName()).build();
    MiniClusterSuite.hbase.getAdmin().createTable(shakespeareTableDesc);

    shakespeareTable = MiniClusterSuite.hbase.getTableByName(TABLE);

    populateShakespeareTable(shakespeareTable);
    LOG.info("Shakespeare table populated.");
  }

  @AfterClass
  public static void teardown() throws IOException {
    MiniClusterSuite.hbase.dropTable(TableName.valueOf(TABLE));
  }

  @Test
  public void countLines() throws Throwable {
    Dataset dataset = new Dataset(shakespeareTable);

    Long result =
        dataset.reduceRows(ZERO, (acc, row) -> acc + 1, ZERO, (acc, count) -> acc + count);
    assert (result > 0);
    LOG.info("Number of lines in Shakespearean plays: {}", result);
  }

  @Test
  public void wordCount() throws Throwable {
    Dataset dataset = new Dataset(shakespeareTable);

    SerializableFunction<byte[], byte[]> extractText = cellValue -> {
      try {
        ShakespeareRecord record =
            new ObjectMapper().readValue(Bytes.toString(cellValue), ShakespeareRecord.class);
        return Bytes.toBytes(record.text);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };

    SerializableFunction<byte[], byte[]> tokenize = cellValue -> {
      try {
        String text = Bytes.toString(cellValue);
        String[] words = text.split("\\W+");
        String[] normalizedWords =
            Arrays.stream(words).map(word -> word.toLowerCase()).toArray(String[]::new);
        return SerdeUtil.serialize(normalizedWords);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };

    SerializableBiFunction<Map<String, Integer>, byte[], Map<String, Integer>> reducer =
        (acc, value) -> {
          try {
            String[] words = SerdeUtil.deserialize(value);
            for (String word : words) {
              acc.put(word, acc.getOrDefault(word, 0) + 1);
            }
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
          return acc;
        };

    SerializableBiFunction<Map<String, Integer>, Map<String, Integer>, Map<String, Integer>> merger =
        (a, b) -> {
          b.forEach((key, value) -> a.merge(key, value, (x, y) -> x + y));
          return a;
        };

    Map<String, Integer> result = dataset.mapCellValues(extractText).mapCellValues(tokenize)
        .reduceCellValues(emptyWordCount(), reducer, emptyWordCount(), merger);

    assert (result.size() > 0);
    LOG.info("Number of distinct words in Shakespearean plays: {}", result.size());
    LOG.info("Total number of words in Shakespearean plays: {}",
        result.values().stream().reduce((a, b) -> a + b).get());
    String mostFrequent = Collections.max(result.entrySet(), Map.Entry.comparingByValue()).getKey();
    LOG.info("Most frequent word ({} occurrences) is '{}'", result.get(mostFrequent), mostFrequent);
  }

  @Test
  public void countSpeakersAndTheirLines() throws Throwable {
    Dataset dataset = new Dataset(shakespeareTable);

    dataset.filterByRowkey(rowkey -> rowkey[0] % 2 == 0).toScanner();

    SerializableBiFunction<Map<String, Integer>, byte[], Map<String, Integer>> reducer =
        (acc, value) -> {
          try {
            ShakespeareRecord record =
                new ObjectMapper().readValue(Bytes.toString(value), ShakespeareRecord.class);
            acc.put(record.speaker, acc.getOrDefault(record.speaker, 0) + 1);
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
          return acc;
        };

    SerializableBiFunction<Map<String, Integer>, Map<String, Integer>, Map<String, Integer>> merger =
        (a, b) -> {
          b.forEach((key, value) -> a.merge(key, value, (x, y) -> x + y));
          return a;
        };

    Map<String, Integer> result =
        dataset.reduceCellValues(emptyWordCount(), reducer, emptyWordCount(), merger);

    assert (result.size() > 0);
    LOG.info("Number of speakers in Shakespearean plays: {}", result.size());
    String mostSpeaker = Collections.max(result.entrySet(), Map.Entry.comparingByValue()).getKey();;
    LOG.info("Most lines ({} in fact) were spoken by {}", result.get(mostSpeaker), mostSpeaker);
  }

  private static void populateShakespeareTable(Table table) throws IOException {

    List<String> plays = loadPlays();

    ArrayList<Put> puts = new ArrayList<>();
    try (Stream<String> stream =
        Files.lines(Paths.get("src/test/resources/shakespeare-cleaned.json"))) {
      stream.forEach(line -> {
        try {
          puts.add(toPut(line, plays));
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      });
    }
    table.put(puts);
  }

  private static class ShakespeareRecord {
    public String play;
    public String lineCoordinates;
    public String speaker;
    public String text;
  }

  private static List<String> loadPlays() throws IOException {
    Scanner sc = new Scanner(new File("src/test/resources/plays.txt"));
    List<String> plays = new ArrayList<String>();
    while (sc.hasNextLine()) {
      plays.add(sc.nextLine());
    }
    sc.close();
    return plays;
  }

  private static Put toPut(String rawRecord, List<String> plays) throws Throwable {
    ShakespeareRecord record = new ObjectMapper().readValue(rawRecord, ShakespeareRecord.class);
    Put put = new Put(lineCoords2rowkey(plays.indexOf(record.play), record.lineCoordinates));
    put.addColumn(TestUtils.CF, TestUtils.QUALIFIER, Bytes.toBytes(rawRecord));
    return put;
  }

  private static byte[] lineCoords2rowkey(int playId, String coords) {
    byte[] rowkey = new byte[16];
    System.arraycopy(Bytes.toBytes(playId), 0, rowkey, 0, 4);
    String[] parts = coords.split("\\.");
    for (int i = 0; i < 3; i++) {
      int parsed = Integer.parseInt(parts[i]);
      System.arraycopy(Bytes.toBytes(parsed), 0, rowkey, 4 + 4 * i, 4);
    }
    return rowkey;
  }
}
