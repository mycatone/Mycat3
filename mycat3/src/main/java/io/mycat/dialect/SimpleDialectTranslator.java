package io.mycat.dialect;

public class SimpleDialectTranslator implements DialectTranslator {

    @Override
    public String translate(String sql, SqlDialect from, SqlDialect to) {
        if (from == to) return sql;
        String upper = sql.trim().toUpperCase();
        if ("SELECT 'X' AS DUMMY".equals(upper)) {
            if (to == SqlDialect.MYSQL) return "SELECT 'X' AS DUMMY";
            if (to == SqlDialect.POSTGRESQL) return "SELECT 'X' AS dummy";
            return sql;
        }
        return sql;
    }
}