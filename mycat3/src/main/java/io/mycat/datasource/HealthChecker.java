package io.mycat.datasource;

import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthChecker.class);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ConnectionPoolManager poolManager;
    private final long intervalMs;

    public HealthChecker(ConnectionPoolManager poolManager, long intervalMs) {
        this.poolManager = poolManager;
        this.intervalMs = intervalMs;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            for (DatabaseEndpoint ep : poolManager.all()) {
                ep.healthCheck().thenAccept(status -> {
                    if (status != DatabaseEndpoint.HealthStatus.ONLINE) {
                        LOGGER.warn("Endpoint {} health: {}", ep.name(), status);
                    }
                });
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdown();
    }
}