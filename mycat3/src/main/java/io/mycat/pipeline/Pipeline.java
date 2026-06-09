package io.mycat.pipeline;

import io.mycat.frontend.QueryResponse;
import java.util.concurrent.CompletableFuture;

public interface Pipeline {
    CompletableFuture<QueryResponse> execute(PipelineContext context);
}