package io.mycat.frontend;

import java.util.List;

public class QueryResponse {
    private final boolean isSelect;
    private final List<String> columns;
    private final List<List<Object>> rows;
    private final long affectedRows;
    private final String error;
    private final long lastInsertId;

    private QueryResponse(boolean isSelect, List<String> columns, List<List<Object>> rows,
                          long affectedRows, String error, long lastInsertId) {
        this.isSelect = isSelect;
        this.columns = columns;
        this.rows = rows;
        this.affectedRows = affectedRows;
        this.error = error;
        this.lastInsertId = lastInsertId;
    }

    public static QueryResponse resultSet(List<String> columns, List<List<Object>> rows) {
        return new QueryResponse(true, columns, rows, 0, null, 0);
    }

    public static QueryResponse ok(long affectedRows) {
        return new QueryResponse(false, null, null, affectedRows, null, 0);
    }

    public static QueryResponse ok(long affectedRows, long lastInsertId) {
        return new QueryResponse(false, null, null, affectedRows, null, lastInsertId);
    }

    public static QueryResponse error(String error) {
        return new QueryResponse(false, null, null, 0, error, 0);
    }

    public boolean isSelect() { return isSelect; }
    public List<String> columns() { return columns; }
    public List<List<Object>> rows() { return rows; }
    public long affectedRows() { return affectedRows; }
    public String error() { return error; }
    public long lastInsertId() { return lastInsertId; }
}