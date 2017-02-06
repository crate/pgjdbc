package org.postgresql.crate;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class ArrayTest extends BasePgJDBCIntegrationTest {

    @Before
    public void before() throws SQLException {
        connection.createStatement().execute("create table if not exists test.arr (int_arr array(int), str_arr array(string))");
    }

    @After
    public void after() throws SQLException {
        connection.createStatement().execute("drop table test.arr");
    }

    @Test
    public void testRetrieveArrays() throws SQLException {
        Statement stmt = connection.createStatement();

        stmt.executeUpdate("insert into test.arr values ([1,2,3], ['A', 'B', 'C'])");

        ResultSet rs = stmt.executeQuery("select int_arr, str_arr from test.arr");
        //Assert.assertTrue(rs.next());

        Array arr = rs.getArray(1);
        Assert.assertEquals(Types.INTEGER, arr.getBaseType());
        Integer intarr[] = (Integer[]) arr.getArray();
        Assert.assertEquals(3, intarr.length);
        Assert.assertEquals(1, intarr[0].intValue());
        Assert.assertEquals(2, intarr[1].intValue());
        Assert.assertEquals(3, intarr[2].intValue());

        arr = rs.getArray(2);
        Assert.assertEquals(Types.VARCHAR, arr.getBaseType());
        String strarr[] = (String[]) arr.getArray();
        Assert.assertEquals(3, strarr.length);
        Assert.assertEquals('A', intarr[0].intValue());
        Assert.assertEquals('B', intarr[1].intValue());
        Assert.assertEquals('C', intarr[2].intValue());

        rs.close();
        stmt.close();

    }
}
