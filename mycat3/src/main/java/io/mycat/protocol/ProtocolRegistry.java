package io.mycat.protocol;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProtocolRegistry {

    private static final Map<String, ProtocolHandler> handlers = new ConcurrentHashMap<>();

    private ProtocolRegistry() {
    }

    public static void register(ProtocolHandler handler) {
        handlers.put(handler.getProtocolName(), handler);
    }

    public static ProtocolHandler getHandler(String protocolName) {
        return handlers.get(protocolName);
    }
}