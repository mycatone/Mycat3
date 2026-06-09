package io.mycat.plugin;

import io.mycat.pipeline.DefaultPipeline;
import io.mycat.pipeline.ExecuteStage;
import io.mycat.pipeline.ParseStage;
import io.mycat.pipeline.Pipeline;
import io.mycat.pipeline.PipelineContext;
import io.mycat.pipeline.PipelineStage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class InterceptorStage implements PipelineStage {
    private final List<SqlInterceptor> interceptors;

    public InterceptorStage(List<SqlInterceptor> interceptors) {
        this.interceptors = new ArrayList<>(interceptors);
    }

    @Override
    public String name() { return "interceptor"; }

    @Override
    public int order() { return 200; }

    @Override
    public CompletableFuture<PipelineContext> process(PipelineContext context) {
        if (context.isShortcut()) {
            return CompletableFuture.completedFuture(context);
        }
        CompletableFuture<PipelineContext> future = CompletableFuture.completedFuture(context);
        for (SqlInterceptor interceptor : interceptors) {
            future = future.thenCompose(interceptor::intercept);
        }
        return future;
    }
}