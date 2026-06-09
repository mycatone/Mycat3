package io.mycat.protocol.api;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import java.util.concurrent.CompletableFuture;

public interface FrontendProtocolHandler {
    String protocolName();
    void handle(NetSocket socket, Buffer initialData);
}