package io.mycat.pipeline;

import io.mycat.frontend.SqlRequest;
import java.util.concurrent.CompletableFuture;

public class RewriteStage implements PipelineStage {
    @Override public String name() { return "rewrite"; }
    @Override public int order() { return 300; }

    @Override
    public CompletableFuture<PipelineContext> process(PipelineContext context) {
        if (context.isShortcut()) {
            return CompletableFuture.completedFuture(context);
        }

        SqlRequest request = context.request();
        if (request.sql() != null) {
            String original = request.sql();
            String rewritten = rewriteSql(original);
            if (!rewritten.equals(original)) {
                context.setAttribute("originalSql", original);
                context.setAttribute("rewrittenSql", rewritten);
                SqlRequest newRequest = new SqlRequest(rewritten, request.type());
                PipelineContext newCtx = new PipelineContext(newRequest, context.session());
                newCtx.attributes().putAll(context.attributes());
                return CompletableFuture.completedFuture(newCtx);
            }
        }
        return CompletableFuture.completedFuture(context);
    }

    private String rewriteSql(String sql) {
        String upper = sql.trim().toUpperCase();
        if (upper.equals("SELECT 1") || upper.equals("SELECT 1;")) {
            return sql;
        }
        if (upper.startsWith("SELECT * FROM DUAL") || upper.startsWith("SELECT * FROM dual")) {
            return "SELECT 'X' AS DUMMY";
        }
        return sql;
    }
}