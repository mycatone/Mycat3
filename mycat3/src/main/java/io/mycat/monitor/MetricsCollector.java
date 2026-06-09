package io.mycat.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsCollector {
    private static final MetricsCollector INSTANCE = new MetricsCollector();

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final AtomicLong totalQueries = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong activeConnections = new AtomicLong(0);

    public static MetricsCollector getInstance() { return INSTANCE; }

    public void recordQuery(String protocol, String sqlType) {
        totalQueries.incrementAndGet();
        increment("query." + protocol + "." + sqlType);
        increment("query." + protocol + ".total");
    }

    public void recordError(String protocol, String errorType) {
        totalErrors.incrementAndGet();
        increment("error." + protocol + "." + errorType);
    }

    public void recordLatency(String protocol, long latencyMs) {
        String key = "latency." + protocol + ".p99";
        getGauge(key).set(latencyMs);
    }

    public void setActiveConnections(long count) {
        activeConnections.set(count);
    }

    public long getTotalQueries() { return totalQueries.get(); }
    public long getTotalErrors() { return totalErrors.get(); }
    public long getActiveConnections() { return activeConnections.get(); }

    public Map<String, Long> getAllMetrics() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, AtomicLong> e : counters.entrySet()) {
            result.put(e.getKey(), e.getValue().get());
        }
        for (Map.Entry<String, AtomicLong> e : gauges.entrySet()) {
            result.put(e.getKey(), e.getValue().get());
        }
        result.put("total.queries", totalQueries.get());
        result.put("total.errors", totalErrors.get());
        result.put("active.connections", activeConnections.get());
        return result;
    }

    public void reset() {
        counters.clear();
        gauges.clear();
        totalQueries.set(0);
        totalErrors.set(0);
        activeConnections.set(0);
    }

    private void increment(String key) {
        getCounter(key).incrementAndGet();
    }

    private AtomicLong getCounter(String key) {
        return counters.computeIfAbsent(key, k -> new AtomicLong(0));
    }

    private AtomicLong getGauge(String key) {
        return gauges.computeIfAbsent(key, k -> new AtomicLong(0));
    }
}