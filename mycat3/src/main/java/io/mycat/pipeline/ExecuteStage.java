package io.mycat.pipeline;

import io.mycat.datasource.ConnectionPoolManager;
import io.mycat.datasource.RealDatabaseEndpoint;
import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.QueryResponse;
import java.util.concurrent.CompletableFuture;

public class ExecuteStage implements PipelineStage {
    private final InMemoryDB db;
    private final ConnectionPoolManager connectionPoolManager;

    public ExecuteStage(InMemoryDB db) {
        this.db = db;
        this.connectionPoolManager = null;
    }

    public ExecuteStage(InMemoryDB db, ConnectionPoolManager connectionPoolManager) {
        this.db = db;
        this.connectionPoolManager = connectionPoolManager;
    }

    @Override
    public String name() { return "execute"; }

    @Override
    public int order() { return 600; }

    @Override
    public CompletableFuture<PipelineContext> process(PipelineContext context) {
        if (context.isShortcut()) {
            return CompletableFuture.completedFuture(context);
        }
        String rawSql = context.request().sql();
        final String sql = rawSql != null ? rawSql : "";

        String targetEndpoint = context.getAttribute("targetEndpoint");
        if (connectionPoolManager != null && targetEndpoint != null) {
            return connectionPoolManager.get(targetEndpoint)
                    .filter(ep -> ep instanceof RealDatabaseEndpoint)
                    .map(ep -> ((RealDatabaseEndpoint) ep).executeQuery(sql)
                            .thenApply(qr -> {
                                context.shortcut(qr);
                                return context;
                            }))
                    .orElseGet(() -> executeWithInMemoryDB(context, sql));
        }

        return executeWithInMemoryDB(context, sql);
    }

    private CompletableFuture<PipelineContext> executeWithInMemoryDB(PipelineContext context, String sql) {
        InMemoryDB.QueryResult result = db.executeQuery(sql);

        QueryResponse qr;
        if (result.error != null) {
            qr = QueryResponse.error(result.error);
        } else if (result.isSelect) {
            qr = QueryResponse.resultSet(result.columns, result.rows);
        } else {
            qr = QueryResponse.ok(result.affectedRows);
        }
        context.shortcut(qr);
        return CompletableFuture.completedFuture(context);
    }
}