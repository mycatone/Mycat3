package io.mycat.protocol.api;

import java.util.Arrays;
import java.util.List;

public class ProtocolCapability {
    private final String protocolName;
    private final int defaultPort;
    private final boolean supportsPreparedStatement;
    private final boolean supportsTransaction;
    private final boolean supportsCursor;
    private final String defaultAuthPlugin;
    private final List<String> supportedAuthPlugins;
    private final String sqlDialect;

    public ProtocolCapability(String protocolName, int defaultPort,
            boolean supportsPreparedStatement, boolean supportsTransaction,
            boolean supportsCursor, String defaultAuthPlugin,
            List<String> supportedAuthPlugins, String sqlDialect) {
        this.protocolName = protocolName;
        this.defaultPort = defaultPort;
        this.supportsPreparedStatement = supportsPreparedStatement;
        this.supportsTransaction = supportsTransaction;
        this.supportsCursor = supportsCursor;
        this.defaultAuthPlugin = defaultAuthPlugin;
        this.supportedAuthPlugins = supportedAuthPlugins;
        this.sqlDialect = sqlDialect;
    }

    public String getProtocolName() { return protocolName; }
    public int getDefaultPort() { return defaultPort; }
    public boolean isSupportsPreparedStatement() { return supportsPreparedStatement; }
    public boolean isSupportsTransaction() { return supportsTransaction; }
    public boolean isSupportsCursor() { return supportsCursor; }
    public String getDefaultAuthPlugin() { return defaultAuthPlugin; }
    public List<String> getSupportedAuthPlugins() { return supportedAuthPlugins; }
    public String getSqlDialect() { return sqlDialect; }

    public static ProtocolCapability mysql() {
        return new ProtocolCapability("mysql", 3306, true, true, true,
            "mysql_native_password", Arrays.asList("mysql_native_password", "caching_sha2_password"), "mysql");
    }
    public static ProtocolCapability postgresql() {
        return new ProtocolCapability("postgresql", 5432, true, true, true,
            "scram-sha-256", Arrays.asList("scram-sha-256", "md5", "password"), "pg");
    }
    public static ProtocolCapability sqlserver() {
        return new ProtocolCapability("sqlserver", 1433, true, true, true,
            "ntlm", Arrays.asList("ntlm", "sql_server"), "tsql");
    }
    public static ProtocolCapability oracle() {
        return new ProtocolCapability("oracle", 1521, true, true, true,
            "native", Arrays.asList("native", "kerberos"), "plsql");
    }
}