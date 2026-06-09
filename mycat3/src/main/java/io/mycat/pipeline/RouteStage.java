package io.mycat.pipeline;

import java.util.concurrent.CompletableFuture;

public class RouteStage implements PipelineStage {
    @Override public String name() { return "route"; }
    @Override public int order() { return 400; }

    @Override
    public CompletableFuture<PipelineContext> process(PipelineContext context) {
        if (context.isShortcut()) {
            return CompletableFuture.completedFuture(context);
        }
        context.setAttribute("routeTarget", "default");
        return CompletableFuture.completedFuture(context);
    }
}