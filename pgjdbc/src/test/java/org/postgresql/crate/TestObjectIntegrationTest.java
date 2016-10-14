package org.postgresql.crate;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestObjectIntegrationTest extends BasePgJDBCIntegrationTest {

  @Before
  public void before() throws SQLException {
    connection.createStatement().execute("create table if not exists test.test (obj object as (n int))");
  }

  @After
  public void after() throws SQLException {
    connection.createStatement().execute("drop table test.test");
  }

  @Test
  public void testSetGetObject() throws SQLException {
    Map<String, Integer> expected = new HashMap<>();
    expected.put("n", 1);
    PreparedStatement statement = connection.prepareStatement("insert into test.test (obj) values (?)");
    statement.setObject(1, expected);
    statement.execute();

    connection.createStatement().execute("refresh table test.test");
    ResultSet resultSet = connection.createStatement().executeQuery("select obj from test.test");
    resultSet.next();

    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) resultSet.getObject(1);
    assertEquals(expected, map);
  }
}
