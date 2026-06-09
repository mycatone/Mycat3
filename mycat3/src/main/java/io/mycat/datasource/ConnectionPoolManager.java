package io.mycat.datasource;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionPoolManager {
    private final Map<String, DatabaseEndpoint> endpoints = new ConcurrentHashMap<>();

    public void register(DatabaseEndpoint endpoint) {
        endpoints.put(endpoint.name(), endpoint);
    }

    public Optional<DatabaseEndpoint> get(String name) {
        return Optional.ofNullable(endpoints.get(name));
    }

    public Collection<DatabaseEndpoint> all() {
        return endpoints.values();
    }

    public CompletableFuture<Void> shutdown() {
        endpoints.clear();
        return CompletableFuture.completedFuture(null);
    }
}