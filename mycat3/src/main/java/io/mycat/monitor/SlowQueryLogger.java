package io.mycat.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;

public class SlowQueryLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlowQueryLogger.class);
    private final long thresholdMs;

    public SlowQueryLogger(long thresholdMs) {
        this.thresholdMs = thresholdMs;
    }

    public void logIfSlow(String sql, String protocol, long elapsedMs) {
        if (elapsedMs > thresholdMs) {
            LOGGER.warn("SLOW_QUERY protocol={} elapsed={}ms sql=[{}]", protocol, elapsedMs, sql);
        }
    }
}