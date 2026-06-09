package io.mycat.pipeline;

import io.mycat.frontend.FrontendSession;
import io.mycat.frontend.QueryResponse;
import io.mycat.frontend.SqlRequest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PipelineContext {
    private final SqlRequest request;
    private final FrontendSession session;
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();
    private QueryResponse response;
    private boolean shortcut;

    public PipelineContext(SqlRequest request, FrontendSession session) {
        this.request = request;
        this.session = session;
    }

    public SqlRequest request() { return request; }
    public FrontendSession session() { return session; }

    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) { return (T) attributes.get(key); }
    public void setAttribute(String key, Object value) { attributes.put(key, value); }

    public void shortcut(QueryResponse response) {
        this.response = response;
        this.shortcut = true;
    }
    public boolean isShortcut() { return shortcut; }
    public QueryResponse shortcutResponse() { return response; }

    public Map<String, Object> attributes() { return attributes; }
}