package io.mycat.datasource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CircuitBreaker {
    private final int failureThreshold;
    private final long recoveryTimeoutMs;
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    private volatile CircuitState state = CircuitState.CLOSED;

    public enum CircuitState { CLOSED, OPEN, HALF_OPEN }

    public CircuitBreaker(int failureThreshold, long recoveryTimeoutMs) {
        this.failureThreshold = failureThreshold;
        this.recoveryTimeoutMs = recoveryTimeoutMs;
    }

    public boolean allowRequest() {
        if (state == CircuitState.CLOSED) return true;
        if (state == CircuitState.OPEN) {
            if (System.currentTimeMillis() - lastFailureTime.get() > recoveryTimeoutMs) {
                state = CircuitState.HALF_OPEN;
                return true;
            }
            return false;
        }
        return true;
    }

    public void recordSuccess() {
        state = CircuitState.CLOSED;
        failureCount.set(0);
    }

    public void recordFailure() {
        lastFailureTime.set(System.currentTimeMillis());
        if (failureCount.incrementAndGet() >= failureThreshold) {
            state = CircuitState.OPEN;
        }
    }

    public CircuitState getState() { return state; }

    public int getFailureCount() { return failureCount.get(); }
}