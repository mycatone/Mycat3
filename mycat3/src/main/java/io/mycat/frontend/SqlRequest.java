package io.mycat.frontend;

import java.util.Optional;

public class SqlRequest {
    private final String sql;
    private final RequestType type;

    public enum RequestType {
        QUERY, PARSE, BIND, EXECUTE, DESCRIBE, PING, QUIT
    }

    public SqlRequest(String sql, RequestType type) {
        this.sql = sql;
        this.type = type;
    }

    public SqlRequest(String sql) {
        this(sql, RequestType.QUERY);
    }

    public String sql() { return sql; }
    public RequestType type() { return type; }
    public Optional<String> sqlOptional() { return Optional.ofNullable(sql); }
}