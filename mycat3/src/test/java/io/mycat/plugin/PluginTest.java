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
import java.util.concurrent.CompletableFuture;

public class PluginTest {

    private FrontendSession session;

    @Before
    public void setUp() {
        InMemoryDB db = new InMemoryDB();
        session = new DefaultFrontendSession(1, db);
    }

    @Test
    public void testAuditPluginMetadata() {
        AuditPlugin plugin = new AuditPlugin();
        Assert.assertEquals("audit", plugin.name());
        Assert.assertEquals("1.0.0", plugin.version());
    }

    @Test
    public void testAuditPluginIntercept() {
        AuditPlugin plugin = new AuditPlugin();
        SqlRequest request = new SqlRequest("SELECT * FROM users");
        PipelineContext ctx = new PipelineContext(request, session);

        CompletableFuture<PipelineContext> future = plugin.intercept(ctx);
        PipelineContext result = future.join();
        Assert.assertNotNull(result);
        Assert.assertFalse(result.isShortcut());
    }

    @Test
    public void testFirewallPluginMetadata() {
        FirewallPlugin plugin = new FirewallPlugin();
        Assert.assertEquals("firewall", plugin.name());
        Assert.assertEquals("1.0.0", plugin.version());
    }

    @Test
    public void testFirewallAllowsNormalSql() {
        FirewallPlugin plugin = new FirewallPlugin();
        SqlRequest request = new SqlRequest("SELECT * FROM users");
        PipelineContext ctx = new PipelineContext(request, session);

        CompletableFuture<PipelineContext> future = plugin.intercept(ctx);
        PipelineContext result = future.join();
        Assert.assertFalse(result.isShortcut());
    }

    @Test
    public void testFirewallBlocksDropTable() {
        FirewallPlugin plugin = new FirewallPlugin();
        SqlRequest request = new SqlRequest("DROP TABLE users");
        PipelineContext ctx = new PipelineContext(request, session);

        CompletableFuture<PipelineContext> future = plugin.intercept(ctx);
        PipelineContext result = future.join();
        Assert.assertTrue(result.isShortcut());
        Assert.assertNotNull(result.shortcutResponse());
        Assert.assertTrue(result.shortcutResponse().error().contains("firewall"));
    }

    @Test
    public void testFirewallBlocksDropDatabase() {
        FirewallPlugin plugin = new FirewallPlugin();
        SqlRequest request = new SqlRequest("DROP DATABASE mydb");
        PipelineContext ctx = new PipelineContext(request, session);

        CompletableFuture<PipelineContext> future = plugin.intercept(ctx);
        PipelineContext result = future.join();
        Assert.assertTrue(result.isShortcut());
    }

    @Test
    public void testFirewallBlocksTruncate() {
        FirewallPlugin plugin = new FirewallPlugin();
        SqlRequest request = new SqlRequest("TRUNCATE TABLE users");
        PipelineContext ctx = new PipelineContext(request, session);

        CompletableFuture<PipelineContext> future = plugin.intercept(ctx);
        PipelineContext result = future.join();
        Assert.assertTrue(result.isShortcut());
    }

    @Test
    public void testFirewallBlocksAlterTable() {
        FirewallPlugin plugin = new FirewallPlugin();
        SqlRequest request = new SqlRequest("ALTER TABLE users ADD COLUMN age INT");
        PipelineContext ctx = new PipelineContext(request, session);

        CompletableFuture<PipelineContext> future = plugin.intercept(ctx);
        PipelineContext result = future.join();
        Assert.assertTrue(result.isShortcut());
    }

    @Test
    public void testFirewallCaseInsensitive() {
        FirewallPlugin plugin = new FirewallPlugin();
        SqlRequest request = new SqlRequest("drop table users");
        PipelineContext ctx = new PipelineContext(request, session);

        CompletableFuture<PipelineContext> future = plugin.intercept(ctx);
        PipelineContext result = future.join();
        Assert.assertTrue(result.isShortcut());
    }

    @Test
    public void testFirewallAllowsInsert() {
        FirewallPlugin plugin = new FirewallPlugin();
        SqlRequest request = new SqlRequest("INSERT INTO users VALUES (1, 'test')");
        PipelineContext ctx = new PipelineContext(request, session);

        CompletableFuture<PipelineContext> future = plugin.intercept(ctx);
        PipelineContext result = future.join();
        Assert.assertFalse(result.isShortcut());
    }

    @Test
    public void testInterceptorStageMetadata() {
        InterceptorStage stage = new InterceptorStage(Arrays.asList(new AuditPlugin()));
        Assert.assertEquals("interceptor", stage.name());
        Assert.assertEquals(200, stage.order());
    }

    @Test
    public void testInterceptorStageWithMultipleInterceptors() {
        InterceptorStage stage = new InterceptorStage(Arrays.asList(
                new FirewallPlugin(), new AuditPlugin()));
        SqlRequest request = new SqlRequest("DROP TABLE users");
        PipelineContext ctx = new PipelineContext(request, session);

        CompletableFuture<PipelineContext> future = stage.process(ctx);
        PipelineContext result = future.join();
        Assert.assertTrue(result.isShortcut());
    }

    @Test
    public void testInterceptorStageSkipsShortcut() {
        InterceptorStage stage = new InterceptorStage(Arrays.asList(new AuditPlugin()));
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        ctx.shortcut(QueryResponse.ok(0));

        CompletableFuture<PipelineContext> future = stage.process(ctx);
        PipelineContext result = future.join();
        Assert.assertTrue(result.isShortcut());
    }

    @Test
    public void testPluginInitAndDestroy() {
        Plugin plugin = new AuditPlugin();
        plugin.init();
        plugin.destroy();
    }
}