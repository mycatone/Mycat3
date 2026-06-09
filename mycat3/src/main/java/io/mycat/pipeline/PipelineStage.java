package io.mycat.pipeline;

import io.mycat.frontend.QueryResponse;
import java.util.concurrent.CompletableFuture;

public interface PipelineStage {
    CompletableFuture<PipelineContext> process(PipelineContext context);
    String name();
    int order();
}