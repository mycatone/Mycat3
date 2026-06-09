package io.mycat.plugin;

import io.mycat.frontend.QueryResponse;
import io.mycat.pipeline.PipelineContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirewallPlugin implements SqlInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirewallPlugin.class);
    private final Set<String> blacklist = new HashSet<>(Arrays.asList(
        "DROP TABLE", "DROP DATABASE", "TRUNCATE TABLE", "ALTER TABLE"
    ));

    @Override
    public String name() { return "firewall"; }

    @Override
    public String version() { return "1.0.0"; }

    @Override
    public CompletableFuture<PipelineContext> intercept(PipelineContext context) {
        if (context.request().sqlOptional().isPresent()) {
            String upper = context.request().sql().toUpperCase();
            for (String blocked : blacklist) {
                if (upper.contains(blocked)) {
                    LOGGER.warn("[FIREWALL] Blocked SQL: {}", context.request().sql());
                    context.shortcut(QueryResponse.error("SQL blocked by firewall: " + blocked));
                    return CompletableFuture.completedFuture(context);
                }
            }
        }
        return CompletableFuture.completedFuture(context);
    }
}