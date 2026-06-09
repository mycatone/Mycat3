package io.mycat.frontend;

import io.mycat.engine.InMemoryDB;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SessionManagerTest {

    private SessionManager sessionManager;
    private SessionFactory sessionFactory;

    @Before
    public void setUp() {
        InMemoryDB db = new InMemoryDB();
        sessionManager = new SessionManager(100, 60000);
        sessionFactory = new SessionFactory(db);
    }

    @Test
    public void testRegisterSession() {
        DefaultFrontendSession session = sessionFactory.create(1);
        boolean result = sessionManager.register(1, session);
        Assert.assertTrue(result);
        Assert.assertEquals(1, sessionManager.activeCount());
    }

    @Test
    public void testUnregisterSession() {
        DefaultFrontendSession session = sessionFactory.create(1);
        sessionManager.register(1, session);
        Assert.assertEquals(1, sessionManager.activeCount());
        sessionManager.unregister(1);
        Assert.assertEquals(0, sessionManager.activeCount());
    }

    @Test
    public void testMaxSessions() {
        SessionManager limited = new SessionManager(2, 60000);
        Assert.assertTrue(limited.register(1, sessionFactory.create(1)));
        Assert.assertTrue(limited.register(2, sessionFactory.create(2)));
        Assert.assertFalse(limited.register(3, sessionFactory.create(3)));
        Assert.assertEquals(2, limited.activeCount());
    }

    @Test
    public void testGetSession() {
        DefaultFrontendSession session = sessionFactory.create(100);
        sessionManager.register(100, session);
        FrontendSession retrieved = sessionManager.get(100);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(100L, retrieved.sessionId());
    }

    @Test
    public void testShutdown() {
        sessionManager.register(1, sessionFactory.create(1));
        sessionManager.register(2, sessionFactory.create(2));
        Assert.assertEquals(2, sessionManager.activeCount());
        sessionManager.shutdown();
        Assert.assertEquals(0, sessionManager.activeCount());
    }

    @Test
    public void testSessionFactoryCreate() {
        DefaultFrontendSession session = sessionFactory.create(1);
        Assert.assertNotNull(session);
        Assert.assertEquals(FrontendSession.State.READY, session.state());
    }
}