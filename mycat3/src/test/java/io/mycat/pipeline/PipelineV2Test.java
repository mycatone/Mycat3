package io.mycat.pipeline;

import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.DefaultFrontendSession;
import io.mycat.frontend.FrontendSession;
import io.mycat.frontend.QueryResponse;
import io.mycat.frontend.SqlRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class PipelineV2Test {

    private InMemoryDB db;
    private FrontendSession session;

    @Before
    public void setUp() {
        db = new InMemoryDB();
        session = new DefaultFrontendSession(1, db);
    }

    @Test
    public void testRewriteStageNoShortcut() {
        SqlRequest request = new SqlRequest("SELECT * FROM travelrecord");
        PipelineContext ctx = new PipelineContext(request, session);
        RewriteStage stage = new RewriteStage();
        PipelineContext result = stage.process(ctx).join();
        Assert.assertFalse(result.isShortcut());
        Assert.assertEquals("SELECT * FROM travelrecord", result.request().sql());
    }

    @Test
    public void testRewriteStageSelectDual() {
        SqlRequest request = new SqlRequest("SELECT * FROM dual");
        PipelineContext ctx = new PipelineContext(request, session);
        RewriteStage stage = new RewriteStage();
        PipelineContext result = stage.process(ctx).join();
        Assert.assertEquals("SELECT 'X' AS DUMMY", result.request().sql());
    }

    @Test
    public void testRewriteStageSelectOne() {
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        RewriteStage stage = new RewriteStage();
        PipelineContext result = stage.process(ctx).join();
        Assert.assertEquals("SELECT 1", result.request().sql());
    }

    @Test
    public void testRewriteStageShortcut() {
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        ctx.shortcut(QueryResponse.ok(0));
        RewriteStage stage = new RewriteStage();
        PipelineContext result = stage.process(ctx).join();
        Assert.assertTrue(result.isShortcut());
        Assert.assertNotNull(result.shortcutResponse());
    }

    @Test
    public void testRewriteStageOrder() {
        RewriteStage stage = new RewriteStage();
        Assert.assertEquals(300, stage.order());
    }

    @Test
    public void testRouteStageOrder() {
        RouteStage stage = new RouteStage();
        Assert.assertEquals(400, stage.order());
    }

    @Test
    public void testRouteStageSetsTarget() {
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        RouteStage stage = new RouteStage();
        PipelineContext result = stage.process(ctx).join();
        Assert.assertEquals("default", result.getAttribute("routeTarget"));
    }

    @Test
    public void testRouteStageShortcut() {
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        ctx.shortcut(QueryResponse.ok(0));
        RouteStage stage = new RouteStage();
        PipelineContext result = stage.process(ctx).join();
        Assert.assertTrue(result.isShortcut());
    }

    @Test
    public void testOptimizeStageOrder() {
        OptimizeStage stage = new OptimizeStage();
        Assert.assertEquals(500, stage.order());
    }

    @Test
    public void testOptimizeStageSetsFlag() {
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        OptimizeStage stage = new OptimizeStage();
        PipelineContext result = stage.process(ctx).join();
        Assert.assertTrue((Boolean) result.getAttribute("optimized"));
    }

    @Test
    public void testOptimizeStageShortcut() {
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        ctx.shortcut(QueryResponse.ok(0));
        OptimizeStage stage = new OptimizeStage();
        PipelineContext result = stage.process(ctx).join();
        Assert.assertTrue(result.isShortcut());
    }

    @Test
    public void testPipelineCompleteMode() {
        Pipeline pipeline = PipelineFactory.createComplete(db);
        Assert.assertNotNull(pipeline);
    }

    @Test
    public void testPipelineCompleteExecute() throws Exception {
        Pipeline pipeline = PipelineFactory.createComplete(db);
        SqlRequest request = new SqlRequest("SELECT * FROM travelrecord");
        PipelineContext ctx = new PipelineContext(request, session);
        QueryResponse response = pipeline.execute(ctx).get(5, TimeUnit.SECONDS);
        Assert.assertNotNull(response);
        Assert.assertNull(response.error());
    }

    @Test
    public void testPipelineWithRewrite() {
        Pipeline pipeline = PipelineFactory.createWithRewrite(db);
        Assert.assertNotNull(pipeline);
    }

    @Test
    public void testPipelineWithRoute() {
        Pipeline pipeline = PipelineFactory.createWithRoute(db);
        Assert.assertNotNull(pipeline);
    }
}