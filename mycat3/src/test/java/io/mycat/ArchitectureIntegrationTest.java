package io.mycat;

import io.mycat.datasource.ConnectionPoolManager;
import io.mycat.datasource.DatabaseEndpoint;
import io.mycat.datasource.MockDatabaseEndpoint;
import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.DefaultFrontendSession;
import io.mycat.frontend.FrontendSession;
import io.mycat.frontend.QueryResponse;
import io.mycat.frontend.SqlRequest;
import io.mycat.pipeline.DefaultPipeline;
import io.mycat.pipeline.ExecuteStage;
import io.mycat.pipeline.ParseStage;
import io.mycat.pipeline.Pipeline;
import io.mycat.pipeline.PipelineContext;
import io.mycat.pipeline.PipelineFactory;
import io.mycat.pipeline.PipelineStage;
import io.mycat.plugin.AuditPlugin;
import io.mycat.plugin.FirewallPlugin;
import io.mycat.plugin.InterceptorStage;
import io.mycat.plugin.SqlInterceptor;
import io.mycat.protocol.ProtocolDetector;
import io.mycat.protocol.ProtocolRegistry;
import io.mycat.protocol.mysql.MySQLProtocolHandler;
import io.mycat.protocol.pg.PGProtocolHandler;
import io.mycat.protocol.tds.TDSProtocolHandler;
import io.mycat.protocol.tns.TNSProtocolHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ArchitectureIntegrationTest {

    private InMemoryDB db;

    @Before
    public void setUp() {
        db = new InMemoryDB();
        db.insert("travelrecord", new Object[]{1, "integration_test"});
    }

    @Test
    public void testFullStackSelect() throws Exception {
        DefaultFrontendSession session = new DefaultFrontendSession(1, db, PipelineFactory.Mode.FULL);

        QueryResponse response = session.executeSql("SELECT * FROM travelrecord");
        Assert.assertNotNull(response);
        Assert.assertNull(response.error());
        Assert.assertTrue(response.isSelect());

        List<String> cols = response.columns();
        Assert.assertTrue(cols.contains("id"));
        Assert.assertTrue(cols.contains("name"));
    }

    @Test
    public void testFullStackInsertThenSelect() throws Exception {
        DefaultFrontendSession session = new DefaultFrontendSession(1, db, PipelineFactory.Mode.FULL);

        QueryResponse insertResp = session.executeSql(
                "INSERT INTO travelrecord (id, name) VALUES (999, 'fullstack')");
        Assert.assertNull(insertResp.error());
        Assert.assertEquals(1, insertResp.affectedRows());

        QueryResponse selectResp = session.executeSql("SELECT * FROM travelrecord");
        Assert.assertNull(selectResp.error());
        Assert.assertTrue(selectResp.rows().size() >= 2);
    }

    @Test
    public void testFirewallPreventsDangerousOperations() {
        DefaultFrontendSession session = new DefaultFrontendSession(
                2, db, PipelineFactory.Mode.WITH_FIREWALL);

        QueryResponse response = session.executeSql("DROP TABLE travelrecord");
        Assert.assertNotNull(response.error());
        Assert.assertTrue(response.error().contains("firewall"));
    }

    @Test
    public void testAuditPipelineDoesNotBlockQueries() {
        DefaultFrontendSession session = new DefaultFrontendSession(
                3, db, PipelineFactory.Mode.WITH_AUDIT);

        QueryResponse response = session.executeSql("SELECT 'audited' AS x");
        Assert.assertNull(response.error());
        Assert.assertTrue(response.isSelect());
        Assert.assertEquals("audited",
                String.valueOf(response.rows().get(0).get(0)));
    }

    @Test
    public void testProtocolRegistryIntegration() {
        ProtocolRegistry.register(new MySQLProtocolHandler());
        ProtocolRegistry.register(new TDSProtocolHandler());
        ProtocolRegistry.register(new TNSProtocolHandler());
        ProtocolRegistry.register(new PGProtocolHandler());

        Assert.assertNotNull(ProtocolRegistry.getHandler("mysql"));
        Assert.assertNotNull(ProtocolRegistry.getHandler("sqlserver"));
        Assert.assertNotNull(ProtocolRegistry.getHandler("oracle"));
        Assert.assertNotNull(ProtocolRegistry.getHandler("postgresql"));
    }

    @Test
    public void testDataEndpointIntegration() {
        ConnectionPoolManager manager = new ConnectionPoolManager();
        manager.register(new MockDatabaseEndpoint("ep1",
                DatabaseEndpoint.DatabaseType.MYSQL, db));
        MockDatabaseEndpoint endpoint = (MockDatabaseEndpoint) manager.get("ep1").get();

        QueryResponse response = endpoint.executeQuery("SELECT * FROM travelrecord");
        Assert.assertNotNull(response);
        Assert.assertNull(response.error());
    }

    @Test
    public void testProtocolDetectionWithRealPatterns() {
        byte[] tdsData = new byte[10];
        tdsData[0] = 0x12;
        Assert.assertEquals("sqlserver", ProtocolDetector.detect(io.vertx.core.buffer.Buffer.buffer(tdsData)));

        byte[] tnsData = new byte[20];
        tnsData[0] = 0x00;
        tnsData[1] = 0x50;
        tnsData[4] = 0x01;
        Assert.assertEquals("oracle", ProtocolDetector.detect(io.vertx.core.buffer.Buffer.buffer(tnsData)));

        byte[] pgData = new byte[15];
        pgData[0] = 0x00;
        pgData[1] = 0x00;
        pgData[2] = 0x00;
        pgData[3] = 0x20;
        pgData[4] = 0x00;
        pgData[5] = 0x03;
        pgData[6] = 0x00;
        pgData[7] = 0x00;
        Assert.assertEquals("postgresql", ProtocolDetector.detect(io.vertx.core.buffer.Buffer.buffer(pgData)));
    }

    @Test
    public void testPipelineWithAllStages() {
        List<PipelineStage> stages = Arrays.asList(
                new ParseStage(),
                new InterceptorStage(Arrays.asList(new FirewallPlugin(), new AuditPlugin())),
                new ExecuteStage(db)
        );
        Pipeline pipeline = new DefaultPipeline(stages);

        DefaultFrontendSession session = new DefaultFrontendSession(1, db);
        SqlRequest request = new SqlRequest("SELECT 'stages' AS test");
        PipelineContext ctx = new PipelineContext(request, session);

        QueryResponse response = pipeline.execute(ctx).join();
        Assert.assertNotNull(response);
        Assert.assertNull(response.error());
        Assert.assertTrue(response.isSelect());
    }

    @Test
    public void testPipelineShortcutChain() {
        List<PipelineStage> stages = Arrays.asList(
                new InterceptorStage(Arrays.asList(new FirewallPlugin())),
                new ExecuteStage(db)
        );
        Pipeline pipeline = new DefaultPipeline(stages);

        DefaultFrontendSession session = new DefaultFrontendSession(1, db);
        SqlRequest request = new SqlRequest("DROP DATABASE production");
        PipelineContext ctx = new PipelineContext(request, session);

        QueryResponse response = pipeline.execute(ctx).join();
        Assert.assertNotNull(response);
        Assert.assertNotNull(response.error());
        Assert.assertTrue(response.error().contains("firewall"));
    }
}