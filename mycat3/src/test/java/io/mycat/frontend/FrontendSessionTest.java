package io.mycat.frontend;

import io.mycat.engine.InMemoryDB;
import io.mycat.pipeline.PipelineFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.Map;

public class FrontendSessionTest {

    private InMemoryDB db;

    @Before
    public void setUp() {
        db = new InMemoryDB();
    }

    @Test
    public void testCreateSession() {
        DefaultFrontendSession session = new DefaultFrontendSession(1, db);
        Assert.assertNotNull(session);
        Assert.assertEquals(1L, session.sessionId());
        Assert.assertEquals("default", session.currentSchema());
        Assert.assertEquals(FrontendSession.State.READY, session.state());
    }

    @Test
    public void testSetSchema() {
        DefaultFrontendSession session = new DefaultFrontendSession(1, db);
        session.setCurrentSchema("testdb");
        Assert.assertEquals("testdb", session.currentSchema());
    }

    @Test
    public void testVariables() {
        DefaultFrontendSession session = new DefaultFrontendSession(1, db);
        Map<String, Object> vars = session.variables();
        Assert.assertNotNull(vars);
        Assert.assertTrue(vars.isEmpty());

        vars.put("autocommit", true);
        Assert.assertTrue((Boolean) session.variables().get("autocommit"));
    }

    @Test
    public void testCloseSession() {
        DefaultFrontendSession session = new DefaultFrontendSession(1, db);
        session.close();
        Assert.assertEquals(FrontendSession.State.CLOSED, session.state());
    }

    @Test
    public void testExecuteSelectSql() {
        DefaultFrontendSession session = new DefaultFrontendSession(1, db);
        QueryResponse response = session.executeSql("SELECT * FROM travelrecord");
        Assert.assertNull(response.error());
        Assert.assertTrue(response.isSelect());
    }

    @Test
    public void testExecuteInsertSql() {
        DefaultFrontendSession session = new DefaultFrontendSession(1, db);
        QueryResponse response = session.executeSql(
                "INSERT INTO travelrecord (id, name) VALUES (100, 'test_user')");
        Assert.assertNull(response.error());
        Assert.assertFalse(response.isSelect());
        Assert.assertEquals(1, response.affectedRows());
    }

    @Test
    public void testExecuteSqlWithPipeline() {
        DefaultFrontendSession session = new DefaultFrontendSession(1, db, PipelineFactory.Mode.FULL);
        QueryResponse response = session.executeSql(
                "INSERT INTO travelrecord (id, name) VALUES (200, 'pipe_test')");
        Assert.assertNull(response.error());
    }

    @Test
    public void testSelectAfterInsert() {
        DefaultFrontendSession session = new DefaultFrontendSession(1, db);
        session.executeSql("INSERT INTO travelrecord (id, name) VALUES (300, 'alice')");
        session.executeSql("INSERT INTO travelrecord (id, name) VALUES (301, 'bob')");

        QueryResponse response = session.executeSql("SELECT * FROM travelrecord");
        Assert.assertNull(response.error());
        Assert.assertTrue(response.isSelect());
        Assert.assertTrue(response.rows().size() >= 2);
    }

    @Test
    public void testSessionWithAuditMode() {
        DefaultFrontendSession session = new DefaultFrontendSession(
                2, db, PipelineFactory.Mode.WITH_AUDIT);
        QueryResponse response = session.executeSql("SELECT 'test_audit' AS result");
        Assert.assertNull(response.error());
    }

    @Test
    public void testSessionWithFirewallMode() {
        DefaultFrontendSession session = new DefaultFrontendSession(
                3, db, PipelineFactory.Mode.WITH_FIREWALL);
        QueryResponse response = session.executeSql("SELECT * FROM travelrecord");
        Assert.assertNull(response.error());
    }

    @Test
    public void testFirewallBlocksDropTable() {
        DefaultFrontendSession session = new DefaultFrontendSession(
                4, db, PipelineFactory.Mode.WITH_FIREWALL);
        QueryResponse response = session.executeSql("DROP TABLE travelrecord");
        Assert.assertNotNull(response.error());
        Assert.assertTrue(response.error().contains("firewall"));
    }
}