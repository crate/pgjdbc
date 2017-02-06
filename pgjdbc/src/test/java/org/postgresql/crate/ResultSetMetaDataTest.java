package org.postgresql.crate;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class ResultSetMetaDataTest extends BasePgJDBCIntegrationTest {

    @Before
    public void before() throws SQLException {
        connection.createStatement().execute("create table if not exists test.arr (int_arr array(int), long_arr array(long), str_arr array(string))");
    }

    @After
    public void after() throws SQLException {
        connection.createStatement().execute("drop table test.arr");
    }

    @Test
    public void testArrayType() throws Exception {
        Statement stmt = connection.createStatement();

        stmt.executeUpdate("insert into test.arr values ([1,2,3], [1,2,3], ['A', 'B', 'C'])");
        stmt.execute("refresh table test.arr");

        ResultSet resultSet = stmt.executeQuery("select int_arr, long_arr, str_arr from test.arr");
        Assert.assertTrue(resultSet.next());

        Array array = resultSet.getArray("int_arr");
        Assert.assertEquals(array.getBaseType(), Types.INTEGER);
        Integer intarr[] = (Integer[]) array.getArray();
        Assert.assertEquals(3, intarr.length);
        Assert.assertEquals(1, intarr[0].intValue());
        Assert.assertEquals(2, intarr[1].intValue());
        Assert.assertEquals(3, intarr[2].intValue());

        array = resultSet.getArray("long_arr");
        Assert.assertEquals(array.getBaseType(), Types.BIGINT);
        Long longarr[] = (Long[]) array.getArray();
        Assert.assertEquals(3, longarr.length);
        Assert.assertEquals(1, longarr[0].longValue());
        Assert.assertEquals(2, longarr[1].longValue());

        array = resultSet.getArray("str_arr");
        Assert.assertEquals(array.getBaseType(), Types.VARCHAR);
        String strarr[] = (String[]) array.getArray();
        Assert.assertEquals(3, strarr.length);
        Assert.assertEquals("A", strarr[0].toString());
        Assert.assertEquals("B", strarr[1].toString());
        Assert.assertEquals("C", strarr[2].toString());

        resultSet.close();
        stmt.close();
    }
}
