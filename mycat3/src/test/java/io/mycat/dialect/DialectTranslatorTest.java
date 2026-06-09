package io.mycat.dialect;

import io.mycat.dialect.ast.ExpressionNode;
import io.mycat.dialect.ast.SelectNode;
import io.mycat.dialect.ast.SqlNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DialectTranslatorTest {

    @Test
    public void testTranslateSameDialect() {
        SimpleDialectTranslator translator = new SimpleDialectTranslator();
        String result = translator.translate("SELECT 1", DialectTranslator.SqlDialect.MYSQL, DialectTranslator.SqlDialect.MYSQL);
        Assert.assertEquals("SELECT 1", result);
    }

    @Test
    public void testTranslateSelectDual() {
        SimpleDialectTranslator translator = new SimpleDialectTranslator();
        String result = translator.translate("SELECT 'X' AS DUMMY", DialectTranslator.SqlDialect.MYSQL, DialectTranslator.SqlDialect.POSTGRESQL);
        Assert.assertEquals("SELECT 'X' AS dummy", result);
    }

    @Test
    public void testSqlNodeType() {
        SqlNode node = new SqlNode(SqlNode.SqlType.SELECT);
        Assert.assertEquals(SqlNode.SqlType.SELECT, node.getType());
    }

    @Test
    public void testSelectNodeColumns() {
        SelectNode select = new SelectNode("users");
        select.addColumn("id");
        select.addColumn("name");
        Assert.assertEquals("users", select.getTableName());
        List<String> columns = select.getColumns();
        Assert.assertEquals(2, columns.size());
        Assert.assertEquals("id", columns.get(0));
        Assert.assertEquals("name", columns.get(1));
    }

    @Test
    public void testExpressionNode() {
        ExpressionNode expr = new ExpressionNode("a + b");
        Assert.assertEquals("a + b", expr.getExpression());
    }
}