package io.mycat.pipeline;

import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.DefaultFrontendSession;
import io.mycat.frontend.FrontendSession;
import io.mycat.frontend.QueryResponse;
import io.mycat.frontend.SqlRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PipelineTest {

    private InMemoryDB db;
    private FrontendSession session;

    @Before
    public void setUp() {
        db = new InMemoryDB();
        session = new DefaultFrontendSession(1, db);
    }

    @Test
    public void testPipelineContextCreation() {
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        Assert.assertNotNull(ctx);
        Assert.assertEquals("SELECT 1", ctx.request().sql());
        Assert.assertSame(session, ctx.session());
        Assert.assertFalse(ctx.isShortcut());
    }

    @Test
    public void testPipelineContextAttributes() {
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        ctx.setAttribute("key1", "value1");
        ctx.setAttribute("key2", 42);

        Assert.assertEquals("value1", ctx.getAttribute("key1"));
        Assert.assertEquals(Integer.valueOf(42), ctx.getAttribute("key2"));
    }

    @Test
    public void testPipelineContextShortcut() {
        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        Assert.assertFalse(ctx.isShortcut());

        QueryResponse shortcutResp = QueryResponse.error("shortcircuit");
        ctx.shortcut(shortcutResp);
        Assert.assertTrue(ctx.isShortcut());
        Assert.assertSame(shortcutResp, ctx.shortcutResponse());
    }

    @Test
    public void testParseStage() {
        ParseStage stage = new ParseStage();
        Assert.assertEquals("parse", stage.name());
        Assert.assertEquals(100, stage.order());

        SqlRequest request = new SqlRequest("SELECT 1");
        PipelineContext ctx = new PipelineContext(request, session);
        CompletableFuture<PipelineContext> future = stage.process(ctx);
        PipelineContext result = future.join();
        Assert.assertNotNull(result);
    }

    @Test
    public void testExecuteStage() {
        ExecuteStage stage = new ExecuteStage(db);
        Assert.assertEquals("execute", stage.name());
        Assert.assertEquals(600, stage.order());

        SqlRequest request = new SqlRequest("SELECT * FROM travelrecord");
        PipelineContext ctx = new PipelineContext(request, session);
        CompletableFuture<PipelineContext> future = stage.process(ctx);
        PipelineContext result = future.join();
        Assert.assertTrue(result.isShortcut());

        QueryResponse response = result.shortcutResponse();
        Assert.assertNotNull(response);
        Assert.assertNull(response.error());
    }

    @Test
    public void testSimplePipelineExecute()
            throws ExecutionException, InterruptedException, TimeoutException {
        Pipeline pipeline = PipelineFactory.createSimple(db);
        SqlRequest request = new SqlRequest("SELECT * FROM travelrecord");
        PipelineContext ctx = new PipelineContext(request, session);

        QueryResponse response = pipeline.execute(ctx).get(5, TimeUnit.SECONDS);
        Assert.assertNotNull(response);
        Assert.assertNull(response.error());
    }

    @Test
    public void testFullPipelineExecute()
            throws ExecutionException, InterruptedException, TimeoutException {
        Pipeline pipeline = PipelineFactory.createFull(db);
        SqlRequest request = new SqlRequest("SELECT 'hello' AS greeting");
        PipelineContext ctx = new PipelineContext(request, session);

        QueryResponse response = pipeline.execute(ctx).get(5, TimeUnit.SECONDS);
        Assert.assertNotNull(response);
        Assert.assertNull(response.error());
        Assert.assertTrue(response.isSelect());
    }

    @Test
    public void testPipelineInsert()
            throws ExecutionException, InterruptedException, TimeoutException {
        Pipeline pipeline = PipelineFactory.createSimple(db);
        SqlRequest request = new SqlRequest(
                "INSERT INTO travelrecord (id, name) VALUES (500, 'pipeline_test')");
        PipelineContext ctx = new PipelineContext(request, session);

        QueryResponse response = pipeline.execute(ctx).get(5, TimeUnit.SECONDS);
        Assert.assertNotNull(response);
        Assert.assertEquals(1L, response.affectedRows());
    }

    @Test
    public void testPipelineDelete()
            throws ExecutionException, InterruptedException, TimeoutException {
        db.insert("travelrecord", new Object[]{600, "to_delete"});

        Pipeline pipeline = PipelineFactory.createSimple(db);
        SqlRequest request = new SqlRequest(
                "DELETE FROM travelrecord WHERE name = 'to_delete'");
        PipelineContext ctx = new PipelineContext(request, session);

        QueryResponse response = pipeline.execute(ctx).get(5, TimeUnit.SECONDS);
        Assert.assertNotNull(response);
    }

    @Test
    public void testPipelineFactoryModes() {
        Assert.assertNotNull(PipelineFactory.createSimple(db));
        Assert.assertNotNull(PipelineFactory.createFull(db));
        Assert.assertNotNull(PipelineFactory.create(db, PipelineFactory.Mode.WITH_AUDIT));
        Assert.assertNotNull(PipelineFactory.create(db, PipelineFactory.Mode.WITH_FIREWALL));
        Assert.assertNotNull(PipelineFactory.create(db, PipelineFactory.Mode.SIMPLE));
    }
}