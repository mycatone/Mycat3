package io.mycat.protocol;

import io.mycat.protocol.api.MycatProtocolBackend;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

public interface ProtocolHandler {
    String getProtocolName();
    int getDefaultPort();
    void handle(NetSocket socket, Buffer initialData);

    /**
     * Inject the shared auth+SQL backend before {@link #handle(NetSocket, Buffer)} fires.
     * Default no-op keeps any historical implementation source-compatible.
     */
    default void setBackend(MycatProtocolBackend backend) {
    }
}