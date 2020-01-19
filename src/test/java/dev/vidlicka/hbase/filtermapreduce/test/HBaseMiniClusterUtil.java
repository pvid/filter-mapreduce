package dev.vidlicka.hbase.filtermapreduce.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;

public class HBaseMiniClusterUtil extends ExternalResource {

  private static final Logger log = Logger.getLogger(HBaseMiniClusterUtil.class);

  private Configuration configuration;
  private Connection connection;
  private Admin admin;
  private HBaseTestingUtility utility;

  public HBaseMiniClusterUtil() {
    utility = new HBaseTestingUtility();
    this.configuration = utility.getConfiguration();

    configuration.unset("hadoop.log.dir");
    configuration.unset("hadoop.tmp.dir");
    configuration.set("dfs.replication", "1");
    configuration.set("dfs.blocksize", "10m");
    configuration.set("hbase.replication", "false");
    configuration.set("hbase.rpc.timeout", "120000");
    configuration.set("hbase.client.retries.number", "3");

    configuration.reloadConfiguration();
  }

  @Override
  protected void before() throws Exception {
    log.info("Starting HBase mini cluster...");
    utility.startMiniCluster(1, true);
    configuration.reloadConfiguration();
    utility.getHBaseCluster().waitForActiveAndReadyMaster();

    this.connection = utility.getConnection();

    this.admin = utility.getHBaseAdmin();
    log.info("Mini cluster started.");
  }

  @Override
  protected void after() {
    try {
      connection.close();
      log.info("Shutting down mini cluster...");
      utility.shutdownMiniCluster();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Drop a table crated in the current HBaseMiniCluster
   * 
   * @param table to be dropped
   */
  public void dropTable(TableName table) throws IOException {
    if (admin.isTableEnabled(table))
      admin.disableTable(table);
    admin.deleteTable(table);
  }

  public Table getTableByName(byte[] tableName) throws IOException {
    return this.getConnection().getTable(TableName.valueOf(tableName));
  }

  public Connection getConnection() {
    return connection;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public HBaseTestingUtility getUtility() {
    return utility;
  }

  public Admin getAdmin() {
    return admin;
  }
}
