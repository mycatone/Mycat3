package io.mycat.pipeline;

import io.mycat.frontend.QueryResponse;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DefaultPipeline implements Pipeline {
    private final List<PipelineStage> stages;

    public DefaultPipeline(List<PipelineStage> stages) {
        this.stages = new ArrayList<>(stages);
        this.stages.sort(Comparator.comparingInt(PipelineStage::order));
    }

    @Override
    public CompletableFuture<QueryResponse> execute(PipelineContext context) {
        CompletableFuture<PipelineContext> future = CompletableFuture.completedFuture(context);
        for (PipelineStage stage : stages) {
            future = future.thenCompose(stage::process);
        }
        return future.thenApply(ctx -> {
            if (ctx.isShortcut() && ctx.shortcutResponse() != null) {
                return ctx.shortcutResponse();
            }
            return QueryResponse.ok(0);
        });
    }
}