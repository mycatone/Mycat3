package io.mycat.plugin;

import io.mycat.frontend.QueryResponse;
import io.mycat.pipeline.PipelineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;

public class RateLimitPlugin implements SqlInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitPlugin.class);
    private final int maxQps;
    private volatile long lastReset = System.currentTimeMillis();
    private volatile int currentCount = 0;

    public RateLimitPlugin(int maxQps) {
        this.maxQps = maxQps;
    }

    @Override public String name() { return "rate-limit"; }
    @Override public String version() { return "1.0.0"; }

    @Override
    public CompletableFuture<PipelineContext> intercept(PipelineContext context) {
        if (context.isShortcut()) {
            return CompletableFuture.completedFuture(context);
        }
        long now = System.currentTimeMillis();
        if (now - lastReset > 1000) {
            lastReset = now;
            currentCount = 0;
        }
        if (currentCount >= maxQps) {
            LOGGER.warn("Rate limit exceeded: {} qps", maxQps);
            context.shortcut(QueryResponse.error("Rate limit exceeded. Max " + maxQps + " QPS"));
        }
        currentCount++;
        return CompletableFuture.completedFuture(context);
    }
}