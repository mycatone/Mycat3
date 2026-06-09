package io.mycat.frontend;

import org.junit.Assert;
import org.junit.Test;
import java.util.Optional;

public class SqlRequestTest {

    @Test
    public void testQueryRequest() {
        SqlRequest request = new SqlRequest("SELECT * FROM users");
        Assert.assertEquals("SELECT * FROM users", request.sql());
        Assert.assertEquals(SqlRequest.RequestType.QUERY, request.type());
        Assert.assertTrue(request.sqlOptional().isPresent());
    }

    @Test
    public void testRequestWithType() {
        SqlRequest request = new SqlRequest("SELECT 1", SqlRequest.RequestType.QUERY);
        Assert.assertEquals(SqlRequest.RequestType.QUERY, request.type());
    }

    @Test
    public void testSqlOptional() {
        SqlRequest request = new SqlRequest(null);
        Assert.assertFalse(request.sqlOptional().isPresent());
    }

    @Test
    public void testAllRequestTypes() {
        for (SqlRequest.RequestType type : SqlRequest.RequestType.values()) {
            SqlRequest request = new SqlRequest("test", type);
            Assert.assertEquals(type, request.type());
        }
    }
}