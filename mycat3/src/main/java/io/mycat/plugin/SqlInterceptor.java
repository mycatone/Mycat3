package io.mycat.plugin;

import io.mycat.pipeline.PipelineContext;
import java.util.concurrent.CompletableFuture;

public interface SqlInterceptor extends Plugin {
    CompletableFuture<PipelineContext> intercept(PipelineContext context);
}