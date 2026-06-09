package io.mycat.dialect.ast;

public class SqlNode {
    private final SqlType type;

    public SqlNode(SqlType type) {
        this.type = type;
    }

    public SqlType getType() { return type; }

    public enum SqlType {
        SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE, OTHER
    }
}