package io.mycat.plugin;

import io.mycat.frontend.SqlRequest;
import io.mycat.pipeline.PipelineContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadBalancerPlugin implements SqlInterceptor {
    private final List<String> endpoints;
    private final AtomicInteger roundRobin = new AtomicInteger(0);
    private final String strategy;

    public LoadBalancerPlugin(List<String> endpoints, String strategy) {
        this.endpoints = endpoints;
        this.strategy = strategy;
    }

    @Override public String name() { return "load-balancer"; }
    @Override public String version() { return "1.0.0"; }

    @Override
    public CompletableFuture<PipelineContext> intercept(PipelineContext context) {
        if (context.isShortcut() || endpoints.isEmpty()) {
            return CompletableFuture.completedFuture(context);
        }
        String target;
        if ("round_robin".equals(strategy)) {
            int index = roundRobin.getAndUpdate(i -> (i + 1) % endpoints.size());
            target = endpoints.get(index);
        } else {
            target = endpoints.get(0);
        }
        context.setAttribute("lbTarget", target);
        return CompletableFuture.completedFuture(context);
    }
}