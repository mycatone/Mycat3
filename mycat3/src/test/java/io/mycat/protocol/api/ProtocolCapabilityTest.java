package io.mycat.protocol.api;

import org.junit.Assert;
import org.junit.Test;

public class ProtocolCapabilityTest {

    @Test
    public void testMysqlCapability() {
        ProtocolCapability cap = ProtocolCapability.mysql();
        Assert.assertEquals(3306, cap.getDefaultPort());
        Assert.assertEquals("mysql", cap.getSqlDialect());
        Assert.assertTrue(cap.isSupportsPreparedStatement());
        Assert.assertTrue(cap.isSupportsTransaction());
        Assert.assertTrue(cap.isSupportsCursor());
    }

    @Test
    public void testPostgresqlCapability() {
        ProtocolCapability cap = ProtocolCapability.postgresql();
        Assert.assertEquals(5432, cap.getDefaultPort());
        Assert.assertEquals("pg", cap.getSqlDialect());
    }

    @Test
    public void testSqlserverCapability() {
        ProtocolCapability cap = ProtocolCapability.sqlserver();
        Assert.assertEquals(1433, cap.getDefaultPort());
        Assert.assertEquals("tsql", cap.getSqlDialect());
    }

    @Test
    public void testOracleCapability() {
        ProtocolCapability cap = ProtocolCapability.oracle();
        Assert.assertEquals(1521, cap.getDefaultPort());
        Assert.assertEquals("plsql", cap.getSqlDialect());
    }
}