package io.mycat.datasource;

import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.QueryResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.Collection;

public class DataSourceTest {

    private ConnectionPoolManager manager;

    @Before
    public void setUp() {
        manager = new ConnectionPoolManager();
    }

    @Test
    public void testRegisterEndpoint() {
        InMemoryDB db = new InMemoryDB();
        MockDatabaseEndpoint endpoint = new MockDatabaseEndpoint(
                "mysql-db-1", DatabaseEndpoint.DatabaseType.MYSQL, db);
        manager.register(endpoint);

        Assert.assertTrue(manager.get("mysql-db-1").isPresent());
        Assert.assertEquals("mysql-db-1", manager.get("mysql-db-1").get().name());
    }

    @Test
    public void testGetNonexistentEndpoint() {
        Assert.assertFalse(manager.get("nonexistent").isPresent());
    }

    @Test
    public void testRegisterMultipleEndpoints() {
        InMemoryDB db = new InMemoryDB();
        manager.register(new MockDatabaseEndpoint("mysql-1",
                DatabaseEndpoint.DatabaseType.MYSQL, db));
        manager.register(new MockDatabaseEndpoint("pg-1",
                DatabaseEndpoint.DatabaseType.POSTGRESQL, db));
        manager.register(new MockDatabaseEndpoint("mssql-1",
                DatabaseEndpoint.DatabaseType.SQLSERVER, db));

        Collection<DatabaseEndpoint> all = manager.all();
        Assert.assertEquals(3, all.size());
    }

    @Test
    public void testEndpointMetadata() {
        InMemoryDB db = new InMemoryDB();
        MockDatabaseEndpoint endpoint = new MockDatabaseEndpoint(
                "test-db", DatabaseEndpoint.DatabaseType.MYSQL, db);
        Assert.assertEquals("test-db", endpoint.name());
        Assert.assertEquals(DatabaseEndpoint.DatabaseType.MYSQL, endpoint.type());
        Assert.assertEquals(0, endpoint.activeConnections());
    }

    @Test
    public void testEndpointHealthCheck() {
        InMemoryDB db = new InMemoryDB();
        MockDatabaseEndpoint endpoint = new MockDatabaseEndpoint(
                "test-db", DatabaseEndpoint.DatabaseType.MYSQL, db);
        DatabaseEndpoint.HealthStatus status = endpoint.healthCheck().join();
        Assert.assertEquals(DatabaseEndpoint.HealthStatus.ONLINE, status);
    }

    @Test
    public void testEndpointExecuteQuery() {
        InMemoryDB db = new InMemoryDB();
        MockDatabaseEndpoint endpoint = new MockDatabaseEndpoint(
                "test-db", DatabaseEndpoint.DatabaseType.MYSQL, db);

        QueryResponse response = endpoint.executeQuery("SELECT * FROM travelrecord");
        Assert.assertNotNull(response);
        Assert.assertNull(response.error());
        Assert.assertTrue(response.isSelect());
    }

    @Test
    public void testAllDatabaseTypes() {
        for (DatabaseEndpoint.DatabaseType type : DatabaseEndpoint.DatabaseType.values()) {
            Assert.assertNotNull(type);
        }
    }

    @Test
    public void testAllHealthStatuses() {
        for (DatabaseEndpoint.HealthStatus status : DatabaseEndpoint.HealthStatus.values()) {
            Assert.assertNotNull(status);
        }
    }

    @Test
    public void testConnectionPoolShutdown() {
        InMemoryDB db = new InMemoryDB();
        manager.register(new MockDatabaseEndpoint("ep1",
                DatabaseEndpoint.DatabaseType.MYSQL, db));
        manager.shutdown().join();
        Assert.assertTrue(manager.all().isEmpty());
    }
}