package io.mycat.engine;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryDB {

    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    public InMemoryDB() {
        createTable("travelrecord", "id", "name");
    }

    public synchronized void createTable(String name, String... columns) {
        tables.putIfAbsent(name.toLowerCase(), new Table(name.toLowerCase(), Arrays.asList(columns)));
    }

    public List<String> getColumns(String tableName) {
        Table t = tables.get(tableName.toLowerCase());
        return t != null ? t.columns : Collections.emptyList();
    }

    public List<List<Object>> selectAll(String tableName) {
        Table t = tables.get(tableName.toLowerCase());
        if (t == null) return Collections.emptyList();
        List<List<Object>> result = new ArrayList<>();
        for (List<Object> row : t.rows) {
            result.add(new ArrayList<>(row));
        }
        return result;
    }

    public int insert(String tableName, Object[] values) {
        Table t = tables.get(tableName.toLowerCase());
        if (t == null) return 0;
        List<Object> row = new ArrayList<>();
        for (int i = 0; i < Math.min(values.length, t.columns.size()); i++) {
            row.add(values[i]);
        }
        t.rows.add(row);
        return 1;
    }

    public int delete(String tableName, String whereCol, Object whereVal) {
        Table t = tables.get(tableName.toLowerCase());
        if (t == null) return 0;
        int colIdx = t.columns.indexOf(whereCol.toLowerCase());
        if (colIdx < 0) return 0;
        int removed = 0;
        for (int i = t.rows.size() - 1; i >= 0; i--) {
            List<Object> row = t.rows.get(i);
            if (colIdx < row.size() && Objects.equals(String.valueOf(row.get(colIdx)), String.valueOf(whereVal))) {
                t.rows.remove(i);
                removed++;
            }
        }
        return removed;
    }

    public QueryResult executeQuery(String sql) {
        sql = sql.trim();
        if (sql.isEmpty()) return QueryResult.error("Empty query");

        String upper = sql.toUpperCase();

        if (upper.startsWith("SELECT")) {
            return handleSelect(sql);
        } else if (upper.startsWith("INSERT")) {
            return handleInsert(sql);
        } else if (upper.startsWith("DELETE")) {
            return handleDelete(sql);
        } else if (upper.startsWith("UPDATE")) {
            return handleUpdate(sql);
        } else if (upper.startsWith("CREATE")) {
            return handleCreate(sql);
        } else if (upper.startsWith("SET") || upper.startsWith("USE") || upper.startsWith("BEGIN") || upper.startsWith("COMMIT")) {
            return QueryResult.ok(0);
        }
        return QueryResult.error("Unsupported query: " + sql);
    }

    private QueryResult handleSelect(String sql) {
        String upper = sql.toUpperCase();
        String afterSelect = sql.substring(6).trim();

        if (afterSelect.startsWith("'") || afterSelect.startsWith("\"")) {
            char quote = afterSelect.charAt(0);
            int endQuote = afterSelect.indexOf(quote, 1);
            if (endQuote > 0) {
                String literal = afterSelect.substring(1, endQuote);
                String alias = "result";
                String rest = afterSelect.substring(endQuote + 1).trim();
                String restUpper = rest.toUpperCase();
                if (restUpper.startsWith("AS ")) {
                    int asEnd = restUpper.indexOf(' ', 3);
                    if (asEnd < 0) asEnd = rest.length();
                    alias = rest.substring(3, asEnd >= 0 ? Math.min(asEnd, rest.length()) : rest.length()).trim();
                }
                return QueryResult.singleRow(literal, alias);
            }
        }

        if (upper.startsWith("SELECT * FROM ")) {
            String tablePart = sql.substring(14).trim();
            String tableName = tablePart.split("\\s")[0].toLowerCase();
            List<List<Object>> rows = selectAll(tableName);
            List<String> cols = getColumns(tableName);
            return QueryResult.rows(cols, rows);
        }

        if (upper.startsWith("SELECT ")) {
            int fromIdx = upper.indexOf(" FROM ");
            if (fromIdx > 0) {
                String colsPart = sql.substring(7, fromIdx).trim();
                String afterFrom = sql.substring(fromIdx + 6).trim();
                String tableName = afterFrom.split("\\s")[0].toLowerCase();
                if (colsPart.equals("*")) {
                    return QueryResult.rows(getColumns(tableName), selectAll(tableName));
                }
            }
        }

        return QueryResult.rows(Arrays.asList("result"), Collections.emptyList());
    }

    private QueryResult handleInsert(String sql) {
        int intoIdx = sql.toUpperCase().indexOf("INTO ");
        if (intoIdx < 0) return QueryResult.error("Invalid INSERT");
        String afterInto = sql.substring(intoIdx + 5).trim();
        int parenOpen = afterInto.indexOf('(');
        String tableName = afterInto.substring(0, parenOpen >= 0 ? parenOpen : afterInto.indexOf(' ')).trim().toLowerCase();
        int valuesIdx = afterInto.toUpperCase().indexOf("VALUES");
        if (valuesIdx < 0) return QueryResult.error("Missing VALUES");
        String valuesPart = afterInto.substring(valuesIdx + 6).trim();
        int vParenOpen = valuesPart.indexOf('(');
        int vParenClose = valuesPart.indexOf(')');
        if (vParenOpen < 0 || vParenClose < 0) return QueryResult.error("Invalid VALUES");
        String valuesStr = valuesPart.substring(vParenOpen + 1, vParenClose);
        String[] valueTokens = splitCSV(valuesStr);
        Object[] values = new Object[valueTokens.length];
        for (int i = 0; i < valueTokens.length; i++) {
            values[i] = unquote(valueTokens[i].trim());
        }
        int affected = insert(tableName, values);
        return QueryResult.ok(affected);
    }

    private QueryResult handleDelete(String sql) {
        int fromIdx = sql.toUpperCase().indexOf("FROM ");
        if (fromIdx < 0) return QueryResult.error("Invalid DELETE");
        String afterFrom = sql.substring(fromIdx + 5).trim();
        int whereIdx = afterFrom.toUpperCase().indexOf("WHERE ");
        String tableName;
        String whereClause = null;
        if (whereIdx >= 0) {
            tableName = afterFrom.substring(0, whereIdx).trim().toLowerCase();
            whereClause = afterFrom.substring(whereIdx + 6).trim();
        } else {
            tableName = afterFrom.trim().toLowerCase();
        }
        if (whereClause != null) {
            String[] parts = whereClause.split("=", 2);
            if (parts.length == 2) {
                String col = parts[0].trim().toLowerCase();
                Object val = unquote(parts[1].trim());
                return QueryResult.ok(delete(tableName, col, val));
            }
        }
        return QueryResult.ok(0);
    }

    private QueryResult handleUpdate(String sql) {
        return QueryResult.ok(0);
    }

    private QueryResult handleCreate(String sql) {
        return QueryResult.ok(0);
    }

    private static String[] splitCSV(String s) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'' && !inQuote) {
                inQuote = true;
                current.append(c);
            } else if (c == '\'' && inQuote) {
                inQuote = false;
                current.append(c);
            } else if (c == ',' && !inQuote) {
                parts.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        parts.add(current.toString().trim());
        return parts.toArray(new String[0]);
    }

    private static Object unquote(String s) {
        if (s.startsWith("'") && s.endsWith("'")) return s.substring(1, s.length() - 1);
        if (s.startsWith("\"") && s.endsWith("\"")) return s.substring(1, s.length() - 1);
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return s;
        }
    }

    public static class Table {
        final String name;
        final List<String> columns;
        final List<List<Object>> rows;

        Table(String name, List<String> columns) {
            this.name = name;
            this.columns = columns;
            this.rows = new CopyOnWriteArrayList<>();
        }
    }

    public static class QueryResult {
        public final List<String> columns;
        public final List<List<Object>> rows;
        public final int affectedRows;
        public final boolean isSelect;
        public final String error;

        QueryResult(List<String> columns, List<List<Object>> rows) {
            this.columns = columns;
            this.rows = rows;
            this.affectedRows = rows.size();
            this.isSelect = true;
            this.error = null;
        }

        QueryResult(int affectedRows) {
            this.columns = Collections.emptyList();
            this.rows = Collections.emptyList();
            this.affectedRows = affectedRows;
            this.isSelect = false;
            this.error = null;
        }

        QueryResult(String error) {
            this.columns = Collections.emptyList();
            this.rows = Collections.emptyList();
            this.affectedRows = 0;
            this.isSelect = false;
            this.error = error;
        }

        public static QueryResult rows(List<String> columns, List<List<Object>> rows) {
            return new QueryResult(columns, rows);
        }

        public static QueryResult singleRow(String value, String alias) {
            return new QueryResult(Collections.singletonList(alias),
                    Collections.singletonList(Collections.singletonList(value)));
        }

        public static QueryResult ok(int affected) {
            return new QueryResult(affected);
        }

        public static QueryResult error(String msg) {
            return new QueryResult(msg);
        }
    }
}