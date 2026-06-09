package io.mycat.pipeline;

import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.QueryResponse;
import java.util.concurrent.CompletableFuture;

public class ParseStage implements PipelineStage {
    @Override
    public String name() { return "parse"; }

    @Override
    public int order() { return 100; }

    @Override
    public CompletableFuture<PipelineContext> process(PipelineContext context) {
        return CompletableFuture.completedFuture(context);
    }
}