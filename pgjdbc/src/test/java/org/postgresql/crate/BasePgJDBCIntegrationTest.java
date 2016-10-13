package org.postgresql.crate;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import io.crate.testing.CrateTestCluster;
import io.crate.testing.CrateTestServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.postgresql.test.TestUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

public class BasePgJDBCIntegrationTest extends RandomizedTest {

  private static final String CRATE_VERSION = "0.56.1";
  private static final String PSQL_PORT = "5433";

  protected static Connection connection;

  @ClassRule
  public static final CrateTestCluster cluster = CrateTestCluster
    .fromVersion(CRATE_VERSION)
    .keepWorkingDir(false)
    .settings(new HashMap<String, Object>() {{
      put("psql.port", PSQL_PORT);
      put("psql.enabled", true);
    }})
    .build();

  @BeforeClass
  public static void beforeClass() throws Throwable {
    new org.postgresql.Driver();
    CrateTestServer server = cluster.randomServer();
    System.setProperty("server", server.crateHost());
    System.setProperty("port", PSQL_PORT);

    Properties props = new Properties();
    connection = TestUtil.openDB(props);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    TestUtil.closeDB(connection);
  }
}
