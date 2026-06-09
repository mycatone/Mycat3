package io.mycat.frontend;

import io.mycat.engine.InMemoryDB;

public class SessionFactory {
    private final InMemoryDB db;

    public SessionFactory(InMemoryDB db) {
        this.db = db;
    }

    public DefaultFrontendSession create(long sessionId) {
        return new DefaultFrontendSession(sessionId, db);
    }
}