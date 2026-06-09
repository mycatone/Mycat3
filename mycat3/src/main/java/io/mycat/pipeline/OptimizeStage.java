package io.mycat.pipeline;

import java.util.concurrent.CompletableFuture;

public class OptimizeStage implements PipelineStage {
    @Override public String name() { return "optimize"; }
    @Override public int order() { return 500; }

    @Override
    public CompletableFuture<PipelineContext> process(PipelineContext context) {
        if (context.isShortcut()) {
            return CompletableFuture.completedFuture(context);
        }
        context.setAttribute("optimized", true);
        return CompletableFuture.completedFuture(context);
    }
}