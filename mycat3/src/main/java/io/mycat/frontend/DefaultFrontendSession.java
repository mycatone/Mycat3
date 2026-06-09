package io.mycat.frontend;

import io.mycat.engine.InMemoryDB;
import io.mycat.pipeline.Pipeline;
import io.mycat.pipeline.PipelineContext;
import io.mycat.pipeline.PipelineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultFrontendSession implements FrontendSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFrontendSession.class);

    private final long sessionId;
    private String currentSchema;
    private State state;
    private final Map<String, Object> variables = new ConcurrentHashMap<>();
    private final Pipeline pipeline;

    public DefaultFrontendSession(long sessionId, InMemoryDB db, PipelineFactory.Mode mode) {
        this.sessionId = sessionId;
        this.currentSchema = "default";
        this.state = State.READY;
        this.pipeline = PipelineFactory.create(db, mode);
    }

    public DefaultFrontendSession(long sessionId, InMemoryDB db) {
        this(sessionId, db, PipelineFactory.Mode.SIMPLE);
    }

    @Override
    public long sessionId() { return sessionId; }

    @Override
    public String currentSchema() { return currentSchema; }

    @Override
    public void setCurrentSchema(String schema) { this.currentSchema = schema; }

    @Override
    public Map<String, Object> variables() { return variables; }

    @Override
    public State state() { return state; }

    public void setState(State state) { this.state = state; }

    @Override
    public CompletableFuture<Void> close() {
        state = State.CLOSED;
        return CompletableFuture.completedFuture(null);
    }

    public QueryResponse executeSql(String sql) {
        SqlRequest request = new SqlRequest(sql);
        PipelineContext ctx = new PipelineContext(request, this);
        try {
            return pipeline.execute(ctx).join();
        } catch (Exception e) {
            LOGGER.error("Pipeline execution error: {}", e.getMessage(), e);
            return QueryResponse.error(e.getMessage());
        }
    }
}