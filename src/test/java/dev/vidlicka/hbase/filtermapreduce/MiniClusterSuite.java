package dev.vidlicka.hbase.filtermapreduce;

import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import dev.vidlicka.hbase.filtermapreduce.dataset.BasicDatasetSuiteElement;
import dev.vidlicka.hbase.filtermapreduce.dataset.ShakespearDatasetSuiteElement;
import dev.vidlicka.hbase.filtermapreduce.filters.BasicFilterFunctionalitySuiteElement;
import dev.vidlicka.hbase.filtermapreduce.reducer.BasicReducerFunctionalitySuiteElement;
import dev.vidlicka.hbase.filtermapreduce.test.HBaseMiniClusterUtil;

@RunWith(Suite.class)
@Suite.SuiteClasses({BasicDatasetSuiteElement.class, BasicFilterFunctionalitySuiteElement.class,
    BasicReducerFunctionalitySuiteElement.class, ShakespearDatasetSuiteElement.class})
public class MiniClusterSuite {
  @ClassRule
  public static HBaseMiniClusterUtil hbase = new HBaseMiniClusterUtil();
}
