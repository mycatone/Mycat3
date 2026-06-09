package io.mycat.frontend;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.List;

public class QueryResponseTest {

    @Test
    public void testResultSet() {
        List<String> columns = java.util.Arrays.asList("id", "name");
        List<List<Object>> rows = java.util.Arrays.asList(
                java.util.Arrays.asList(1, "Alice"),
                java.util.Arrays.asList(2, "Bob")
        );

        QueryResponse response = QueryResponse.resultSet(columns, rows);
        Assert.assertTrue(response.isSelect());
        Assert.assertEquals(2, response.columns().size());
        Assert.assertEquals(2, response.rows().size());
        Assert.assertNull(response.error());
    }

    @Test
    public void testOk() {
        QueryResponse response = QueryResponse.ok(5);
        Assert.assertFalse(response.isSelect());
        Assert.assertEquals(5L, response.affectedRows());
        Assert.assertNull(response.error());
    }

    @Test
    public void testOkWithInsertId() {
        QueryResponse response = QueryResponse.ok(1, 42);
        Assert.assertEquals(1L, response.affectedRows());
        Assert.assertEquals(42L, response.lastInsertId());
    }

    @Test
    public void testError() {
        QueryResponse response = QueryResponse.error("Something went wrong");
        Assert.assertFalse(response.isSelect());
        Assert.assertEquals("Something went wrong", response.error());
        Assert.assertEquals(0L, response.affectedRows());
    }
}