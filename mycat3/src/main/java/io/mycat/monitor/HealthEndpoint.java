package io.mycat.monitor;

import java.util.Map;

public class HealthEndpoint {

    public HealthResponse health() {
        return new HealthResponse("UP", System.currentTimeMillis());
    }

    public HealthResponse ready() {
        return new HealthResponse("READY", System.currentTimeMillis());
    }

    public MetricsResponse metrics() {
        Map<String, Long> metrics = MetricsCollector.getInstance().getAllMetrics();
        return new MetricsResponse("ok", metrics, System.currentTimeMillis());
    }

    public static class HealthResponse {
        private final String status;
        private final long timestamp;
        public HealthResponse(String status, long timestamp) {
            this.status = status; this.timestamp = timestamp;
        }
        public String getStatus() { return status; }
        public long getTimestamp() { return timestamp; }
    }

    public static class MetricsResponse {
        private final String status;
        private final Map<String, Long> metrics;
        private final long timestamp;
        public MetricsResponse(String status, Map<String, Long> metrics, long timestamp) {
            this.status = status; this.metrics = metrics; this.timestamp = timestamp;
        }
        public String getStatus() { return status; }
        public Map<String, Long> getMetrics() { return metrics; }
        public long getTimestamp() { return timestamp; }
    }
}