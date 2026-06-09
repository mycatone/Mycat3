package io.mycat.plugin;

import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.DefaultFrontendSession;
import io.mycat.frontend.FrontendSession;
import io.mycat.frontend.QueryResponse;
import io.mycat.frontend.SqlRequest;
import io.mycat.pipeline.PipelineContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class PluginV2Test {

    private InMemoryDB db;
    private FrontendSession session;

    @Before
    public void setUp() {
        db = new InMemoryDB();
        session = new DefaultFrontendSession(1, db);
    }

    @Test
    public void testRateLimitPluginMetadata() {
        RateLimitPlugin plugin = new RateLimitPlugin(100);
        Assert.assertEquals("rate-limit", plugin.name());
        Assert.assertEquals("1.0.0", plugin.version());
    }

    @Test
    public void testRateLimitAllowsWithinLimit() {
        RateLimitPlugin plugin = new RateLimitPlugin(100);
        for (int i = 0; i < 5; i++) {
            SqlRequest request = new SqlRequest("SELECT " + i);
            PipelineContext ctx = new PipelineContext(request, session);
            PipelineContext result = plugin.intercept(ctx).join();
            Assert.assertFalse(result.isShortcut());
        }
    }

    @Test
    public void testRateLimitBlocksOverLimit() {
        RateLimitPlugin plugin = new RateLimitPlugin(1);
        SqlRequest req1 = new SqlRequest("SELECT 1");
        PipelineContext ctx1 = new PipelineContext(req1, session);
        PipelineContext result1 = plugin.intercept(ctx1).join();
        Assert.assertFalse(result1.isShortcut());

        SqlRequest req2 = new SqlRequest("SELECT 2");
        PipelineContext ctx2 = new PipelineContext(req2, session);
        PipelineContext result2 = plugin.intercept(ctx2).join();
        Assert.assertTrue(result2.isShortcut());
        Assert.assertNotNull(result2.shortcutResponse());
        Assert.assertTrue(result2.shortcutResponse().error().contains("exceeded"));
    }

    @Test
    public void testRateLimitShortcut() {
        RateLimitPlugin plugin = new RateLimitPlugin(1);
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        ctx.shortcut(QueryResponse.ok(0));
        PipelineContext result = plugin.intercept(ctx).join();
        Assert.assertTrue(result.isShortcut());
    }

    @Test
    public void testLoadBalancerMetadata() {
        LoadBalancerPlugin plugin = new LoadBalancerPlugin(
                Arrays.asList("A"), "round_robin");
        Assert.assertEquals("load-balancer", plugin.name());
        Assert.assertEquals("1.0.0", plugin.version());
    }

    @Test
    public void testLoadBalancerRoundRobin() {
        LoadBalancerPlugin plugin = new LoadBalancerPlugin(
                Arrays.asList("A", "B", "C"), "round_robin");
        String[] expected = {"A", "B", "C"};
        for (int i = 0; i < 3; i++) {
            SqlRequest request = new SqlRequest("SELECT 1");
            PipelineContext ctx = new PipelineContext(request, session);
            PipelineContext result = plugin.intercept(ctx).join();
            Assert.assertFalse(result.isShortcut());
            Assert.assertEquals(expected[i], result.getAttribute("lbTarget"));
        }
    }

    @Test
    public void testLoadBalancerRoundRobinWraps() {
        LoadBalancerPlugin plugin = new LoadBalancerPlugin(
                Arrays.asList("A", "B", "C"), "round_robin");
        for (int i = 0; i < 3; i++) {
            SqlRequest request = new SqlRequest("SELECT 1");
            PipelineContext ctx = new PipelineContext(request, session);
            plugin.intercept(ctx).join();
        }
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        PipelineContext result = plugin.intercept(ctx).join();
        Assert.assertEquals("A", result.getAttribute("lbTarget"));
    }

    @Test
    public void testLoadBalancerEmptyEndpoints() {
        LoadBalancerPlugin plugin = new LoadBalancerPlugin(
                Collections.<String>emptyList(), "round_robin");
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        PipelineContext result = plugin.intercept(ctx).join();
        Assert.assertFalse(result.isShortcut());
        Assert.assertNull(result.getAttribute("lbTarget"));
    }

    @Test
    public void testLoadBalancerShortcut() {
        LoadBalancerPlugin plugin = new LoadBalancerPlugin(
                Arrays.asList("A"), "round_robin");
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        ctx.shortcut(QueryResponse.ok(0));
        PipelineContext result = plugin.intercept(ctx).join();
        Assert.assertTrue(result.isShortcut());
    }

    @Test
    public void testPluginContextAttributes() {
        PluginContext pc = new PluginContext();
        pc.setAttribute("key1", "value1");
        pc.setAttribute("key2", 42);
        Assert.assertEquals("value1", pc.getAttribute("key1"));
        Assert.assertEquals(42, pc.getAttribute("key2"));
    }

    @Test
    public void testPluginLoaderRegister() {
        PluginLoader loader = new PluginLoader();
        loader.register(new AuditPlugin());
        Assert.assertEquals(1, loader.getPlugins().size());
        Assert.assertEquals("audit", loader.getPlugins().get(0).name());
    }

    @Test
    public void testPluginLoaderShutdown() {
        PluginLoader loader = new PluginLoader();
        loader.register(new AuditPlugin());
        loader.register(new FirewallPlugin());
        int size = loader.getPlugins().size();
        Assert.assertTrue(size >= 2);
        loader.shutdown();
    }
}