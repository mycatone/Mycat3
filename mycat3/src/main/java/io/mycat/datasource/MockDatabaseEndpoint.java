package io.mycat.datasource;

import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.QueryResponse;
import java.util.concurrent.CompletableFuture;

public class MockDatabaseEndpoint implements DatabaseEndpoint {
    private final String name;
    private final DatabaseType type;
    private final InMemoryDB db;

    public MockDatabaseEndpoint(String name, DatabaseType type, InMemoryDB db) {
        this.name = name;
        this.type = type;
        this.db = db;
    }

    @Override
    public String name() { return name; }

    @Override
    public DatabaseType type() { return type; }

    @Override
    public CompletableFuture<HealthStatus> healthCheck() {
        return CompletableFuture.completedFuture(HealthStatus.ONLINE);
    }

    @Override
    public int activeConnections() { return 0; }

    public QueryResponse executeQuery(String sql) {
        InMemoryDB.QueryResult result = db.executeQuery(sql);
        if (result.error != null) {
            return QueryResponse.error(result.error);
        } else if (result.isSelect) {
            return QueryResponse.resultSet(result.columns, result.rows);
        } else {
            return QueryResponse.ok(result.affectedRows);
        }
    }
}