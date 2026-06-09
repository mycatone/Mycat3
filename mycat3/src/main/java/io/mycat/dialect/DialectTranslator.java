package io.mycat.dialect;

public interface DialectTranslator {
    String translate(String sql, SqlDialect from, SqlDialect to);

    enum SqlDialect {
        MYSQL, POSTGRESQL, TSQL, PLSQL, STANDARD
    }
}