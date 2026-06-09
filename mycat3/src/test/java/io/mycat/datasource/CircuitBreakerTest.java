package io.mycat.datasource;

import org.junit.Assert;
import org.junit.Test;

public class CircuitBreakerTest {

    @Test
    public void testInitialStateClosed() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000);
        Assert.assertEquals(CircuitBreaker.CircuitState.CLOSED, cb.getState());
    }

    @Test
    public void testAllowWhenClosed() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000);
        Assert.assertTrue(cb.allowRequest());
    }

    @Test
    public void testOpenAfterThreshold() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000);
        cb.recordFailure();
        cb.recordFailure();
        Assert.assertEquals(CircuitBreaker.CircuitState.CLOSED, cb.getState());
        cb.recordFailure();
        Assert.assertEquals(CircuitBreaker.CircuitState.OPEN, cb.getState());
        Assert.assertFalse(cb.allowRequest());
    }

    @Test
    public void testHalfOpenAfterTimeout() throws Exception {
        CircuitBreaker cb = new CircuitBreaker(2, 1);
        cb.recordFailure();
        cb.recordFailure();
        Assert.assertEquals(CircuitBreaker.CircuitState.OPEN, cb.getState());
        Thread.sleep(2);
        Assert.assertTrue(cb.allowRequest());
        Assert.assertEquals(CircuitBreaker.CircuitState.HALF_OPEN, cb.getState());
    }

    @Test
    public void testCloseAfterSuccess() throws Exception {
        CircuitBreaker cb = new CircuitBreaker(2, 1);
        cb.recordFailure();
        cb.recordFailure();
        Assert.assertEquals(CircuitBreaker.CircuitState.OPEN, cb.getState());
        Thread.sleep(2);
        Assert.assertTrue(cb.allowRequest());
        Assert.assertEquals(CircuitBreaker.CircuitState.HALF_OPEN, cb.getState());
        cb.recordSuccess();
        Assert.assertEquals(CircuitBreaker.CircuitState.CLOSED, cb.getState());
    }

    @Test
    public void testMultipleFailures() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000);
        cb.recordFailure();
        cb.recordFailure();
        cb.recordFailure();
        Assert.assertEquals(CircuitBreaker.CircuitState.OPEN, cb.getState());
        cb.recordFailure();
        cb.recordFailure();
        Assert.assertEquals(CircuitBreaker.CircuitState.OPEN, cb.getState());
        Assert.assertEquals(5, cb.getFailureCount());
    }
}