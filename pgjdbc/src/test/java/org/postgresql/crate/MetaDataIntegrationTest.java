package org.postgresql.crate;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MetaDataIntegrationTest extends BasePgJDBCIntegrationTest {

  @Before
  public void before() throws SQLException {
    connection.createStatement().execute("create table if not exists test.cluster (arr array(int), name string)");
  }

  @After
  public void after() throws SQLException {
    connection.createStatement().execute("drop table test.cluster");
  }

  @Test
  public void testGetTables() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet rs = metaData.getTables("", "sys", "cluster", null);

    assertTrue(rs.next());
    assertEquals("sys", rs.getString("TABLE_SCHEM"));
    assertEquals("cluster", rs.getString("TABLE_NAME"));
    assertEquals("SYSTEM TABLE", rs.getString("TABLE_TYPE"));
    assertFalse(rs.next());
  }

  @Test
  public void testGetTablesWithNullSchema() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet rs = metaData.getTables("", null, "clus%", null);

    assertTrue(rs.next());
    assertEquals("sys", rs.getString("TABLE_SCHEM"));
    assertEquals("cluster", rs.getString("TABLE_NAME"));
    assertEquals("SYSTEM TABLE", rs.getString("TABLE_TYPE"));

    assertTrue(rs.next());
    assertEquals("test", rs.getString("TABLE_SCHEM"));
    assertEquals("cluster", rs.getString("TABLE_NAME"));
    assertEquals("TABLE", rs.getString("TABLE_TYPE"));
    assertFalse(rs.next());
  }

  @Test
  public void testGetTablesWithEmptySchema() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet rs = metaData.getTables("", "", "clust%", null);
    assertFalse(rs.next());
  }

  @Test
  public void testGetColumns() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet rs = metaData.getColumns("", "test", "clus%", "ar%");

    assertTrue(rs.next());
    assertEquals("test", rs.getString("TABLE_SCHEM"));
    assertEquals("cluster", rs.getString("TABLE_NAME"));
    assertEquals("arr", rs.getString("COLUMN_NAME"));
    assertEquals("integer_array", rs.getString("TYPE_NAME"));
    assertEquals(Types.ARRAY, rs.getInt("DATA_TYPE"));
    assertEquals(1, rs.getInt("ORDINAL_POSITION"));
    assertFalse(rs.next());
  }

  @Test
  public void testGetColumnsWithEmptySchema() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet rs = metaData.getTables("", "", "clust%", null);
    assertFalse(rs.next());
  }

  @Test
  public void testGetColumnsWithNullSchema() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet rs = metaData.getColumns("", null, "clus%", "name");

    assertTrue(rs.next());
    assertEquals("sys", rs.getString("TABLE_SCHEM"));
    assertEquals("cluster", rs.getString("TABLE_NAME"));
    assertEquals("name", rs.getString("COLUMN_NAME"));

    assertTrue(rs.next());
    assertEquals("test", rs.getString("TABLE_SCHEM"));
    assertEquals("cluster", rs.getString("TABLE_NAME"));
    assertEquals("name", rs.getString("COLUMN_NAME"));
    assertFalse(rs.next());
  }
}
