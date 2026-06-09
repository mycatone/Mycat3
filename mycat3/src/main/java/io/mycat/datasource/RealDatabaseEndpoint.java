package io.mycat.datasource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.mycat.frontend.QueryResponse;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class RealDatabaseEndpoint implements DatabaseEndpoint {
    private final String name;
    private final DatabaseType type;
    private final HikariDataSource dataSource;
    private HealthStatus currentStatus = HealthStatus.UNKNOWN;

    public RealDatabaseEndpoint(String name, DatabaseType type, String jdbcUrl, String username, String password) {
        this.name = name;
        this.type = type;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMinimumIdle(2);
        config.setMaximumPoolSize(20);
        config.setConnectionTimeout(10000);
        config.setIdleTimeout(300000);
        config.setMaxLifetime(600000);
        config.setPoolName("mycat-" + name);
        this.dataSource = new HikariDataSource(config);
        this.currentStatus = HealthStatus.ONLINE;
    }

    public RealDatabaseEndpoint(String name, DatabaseType type, HikariDataSource dataSource) {
        this.name = name;
        this.type = type;
        this.dataSource = dataSource;
        this.currentStatus = HealthStatus.ONLINE;
    }

    @Override
    public String name() { return name; }

    @Override
    public DatabaseType type() { return type; }

    @Override
    public CompletableFuture<HealthStatus> healthCheck() {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {
                currentStatus = rs.next() ? HealthStatus.ONLINE : HealthStatus.DEGRADED;
            } catch (Exception e) {
                currentStatus = HealthStatus.OFFLINE;
            }
            return currentStatus;
        });
    }

    @Override
    public int activeConnections() {
        return dataSource.getHikariPoolMXBean() != null
            ? dataSource.getHikariPoolMXBean().getActiveConnections() : 0;
    }

    public CompletableFuture<QueryResponse> executeQuery(String sql) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                boolean isResultSet = stmt.execute(sql);
                if (isResultSet) {
                    ResultSet rs = stmt.getResultSet();
                    ResultSetMetaData meta = rs.getMetaData();
                    int colCount = meta.getColumnCount();
                    List<String> columns = new ArrayList<>();
                    for (int i = 1; i <= colCount; i++) {
                        columns.add(meta.getColumnName(i));
                    }
                    List<List<Object>> rows = new ArrayList<>();
                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();
                        for (int i = 1; i <= colCount; i++) {
                            row.add(rs.getObject(i));
                        }
                        rows.add(row);
                    }
                    return QueryResponse.resultSet(columns, rows);
                } else {
                    return QueryResponse.ok(stmt.getUpdateCount());
                }
            } catch (Exception e) {
                return QueryResponse.error(e.getMessage());
            }
        });
    }

    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
}