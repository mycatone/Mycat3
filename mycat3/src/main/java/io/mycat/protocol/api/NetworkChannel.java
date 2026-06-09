package io.mycat.protocol.api;

import io.vertx.core.buffer.Buffer;
import java.util.concurrent.CompletableFuture;

public interface NetworkChannel {
    String id();
    String remoteAddress();
    CompletableFuture<Void> write(Buffer data);
    CompletableFuture<Void> close();
}