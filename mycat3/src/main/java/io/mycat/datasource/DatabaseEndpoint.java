package io.mycat.datasource;

import java.util.concurrent.CompletableFuture;

public interface DatabaseEndpoint {

    enum DatabaseType { MYSQL, POSTGRESQL, SQLSERVER, ORACLE }

    enum HealthStatus { ONLINE, DEGRADED, OFFLINE, UNKNOWN }

    String name();
    DatabaseType type();
    CompletableFuture<HealthStatus> healthCheck();
    int activeConnections();
}