package io.mycat.frontend;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface FrontendSession {
    long sessionId();
    String currentSchema();
    void setCurrentSchema(String schema);
    Map<String, Object> variables();
    CompletableFuture<Void> close();

    enum State { CONNECTING, AUTHENTICATING, READY, BUSY, CLOSING, CLOSED }
    State state();
}