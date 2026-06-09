package io.mycat.protocol.api;

public class ProtocolInfo {
    private final String name;
    private final int defaultPort;

    public ProtocolInfo(String name, int defaultPort) {
        this.name = name;
        this.defaultPort = defaultPort;
    }

    public String getName() { return name; }
    public int getDefaultPort() { return defaultPort; }
}