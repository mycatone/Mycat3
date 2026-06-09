package io.mycat.monitor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.Map;

public class MonitorTest {

    private MetricsCollector collector;
    private SlowQueryLogger slowQueryLogger;
    private HealthEndpoint healthEndpoint;

    @Before
    public void setUp() {
        collector = MetricsCollector.getInstance();
        collector.reset();
        slowQueryLogger = new SlowQueryLogger(10);
        healthEndpoint = new HealthEndpoint();
    }

    @Test
    public void testRecordQuery() {
        collector.recordQuery("mysql", "select");
        Assert.assertTrue(collector.getTotalQueries() > 0);
    }

    @Test
    public void testRecordError() {
        collector.recordError("mysql", "timeout");
        Assert.assertTrue(collector.getTotalErrors() > 0);
    }

    @Test
    public void testSetActiveConnections() {
        collector.setActiveConnections(5);
        Assert.assertEquals(5L, collector.getActiveConnections());
    }

    @Test
    public void testGetAllMetrics() {
        collector.recordQuery("mysql", "select");
        Map<String, Long> metrics = collector.getAllMetrics();
        Assert.assertFalse(metrics.isEmpty());
    }

    @Test
    public void testReset() {
        collector.recordQuery("mysql", "select");
        Assert.assertTrue(collector.getTotalQueries() > 0);
        collector.reset();
        Assert.assertEquals(0L, collector.getTotalQueries());
    }

    @Test
    public void testRecordQueryByProtocol() {
        collector.recordQuery("mysql", "select");
        Map<String, Long> metrics = collector.getAllMetrics();
        Assert.assertNotNull(metrics.get("query.mysql.select"));
    }

    @Test
    public void testSlowQueryLoggerLogs() {
        slowQueryLogger.logIfSlow("SELECT * FROM test", "mysql", 50);
    }

    @Test
    public void testHealthEndpointHealth() {
        HealthEndpoint.HealthResponse response = healthEndpoint.health();
        Assert.assertEquals("UP", response.getStatus());
    }

    @Test
    public void testHealthEndpointReady() {
        HealthEndpoint.HealthResponse response = healthEndpoint.ready();
        Assert.assertEquals("READY", response.getStatus());
    }

    @Test
    public void testHealthEndpointMetrics() {
        HealthEndpoint.MetricsResponse response = healthEndpoint.metrics();
        Assert.assertEquals("ok", response.getStatus());
        Assert.assertNotNull(response.getMetrics());
    }
}