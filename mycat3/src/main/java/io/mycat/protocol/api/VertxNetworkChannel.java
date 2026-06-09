package io.mycat.protocol.api;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

public class VertxNetworkChannel implements NetworkChannel {
    private final NetSocket socket;

    public VertxNetworkChannel(NetSocket socket) {
        this.socket = socket;
    }

    @Override
    public String id() {
        return socket.remoteAddress().toString();
    }

    @Override
    public String remoteAddress() {
        return socket.remoteAddress().toString();
    }

    @Override
    public java.util.concurrent.CompletableFuture<Void> write(Buffer data) {
        socket.write(data);
        return java.util.concurrent.CompletableFuture.completedFuture(null);
    }

    @Override
    public java.util.concurrent.CompletableFuture<Void> close() {
        socket.close();
        return java.util.concurrent.CompletableFuture.completedFuture(null);
    }
}