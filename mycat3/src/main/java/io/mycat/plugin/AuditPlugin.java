package io.mycat.plugin;

import io.mycat.pipeline.PipelineContext;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditPlugin implements SqlInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditPlugin.class);

    @Override
    public String name() { return "audit"; }

    @Override
    public String version() { return "1.0.0"; }

    @Override
    public CompletableFuture<PipelineContext> intercept(PipelineContext context) {
        if (context.request().sqlOptional().isPresent()) {
            LOGGER.info("[AUDIT] schema={}, sql={}",
                context.session().currentSchema(),
                context.request().sql());
        }
        return CompletableFuture.completedFuture(context);
    }
}