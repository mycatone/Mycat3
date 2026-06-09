package io.mycat.dialect.ast;

import java.util.ArrayList;
import java.util.List;

public class SelectNode extends SqlNode {
    private final List<String> columns = new ArrayList<>();
    private final String tableName;

    public SelectNode(String tableName) {
        super(SqlType.SELECT);
        this.tableName = tableName;
    }

    public List<String> getColumns() { return columns; }
    public String getTableName() { return tableName; }
    public void addColumn(String column) { columns.add(column); }
}