package io.mycat.frontend;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);
    private final Map<Long, FrontendSession> sessions = new ConcurrentHashMap<>();
    private final int maxSessions;
    private final long idleTimeoutMs;

    public SessionManager(int maxSessions, long idleTimeoutMs) {
        this.maxSessions = maxSessions;
        this.idleTimeoutMs = idleTimeoutMs;
    }

    public boolean register(long sessionId, FrontendSession session) {
        if (sessions.size() >= maxSessions) {
            LOGGER.warn("Max sessions reached: {}", maxSessions);
            return false;
        }
        sessions.put(sessionId, session);
        LOGGER.info("Session registered: id={}, total={}", sessionId, sessions.size());
        return true;
    }

    public void unregister(long sessionId) {
        sessions.remove(sessionId);
        LOGGER.info("Session unregistered: id={}, total={}", sessionId, sessions.size());
    }

    public FrontendSession get(long sessionId) {
        return sessions.get(sessionId);
    }

    public int activeCount() {
        return sessions.size();
    }

    public void shutdown() {
        LOGGER.info("Shutting down {} sessions", sessions.size());
        for (FrontendSession session : sessions.values()) {
            session.close();
        }
        sessions.clear();
    }
}