package io.mycat.engine;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.List;

public class InMemoryDBTest {

    private InMemoryDB db;

    @Before
    public void setUp() {
        db = new InMemoryDB();
    }

    @Test
    public void testDefaultTableExists() {
        List<String> cols = db.getColumns("travelrecord");
        Assert.assertNotNull(cols);
        Assert.assertEquals(2, cols.size());
        Assert.assertTrue(cols.contains("id"));
        Assert.assertTrue(cols.contains("name"));
    }

    @Test
    public void testCreateTable() {
        db.createTable("test_table", "col1", "col2", "col3");
        List<String> cols = db.getColumns("test_table");
        Assert.assertEquals(3, cols.size());
    }

    @Test
    public void testInsertAndSelect() {
        db.createTable("users", "id", "name", "age");
        int affected = db.insert("users", new Object[]{1, "Alice", 30});
        Assert.assertEquals(1, affected);

        List<List<Object>> rows = db.selectAll("users");
        Assert.assertEquals(1, rows.size());
        Assert.assertEquals("Alice", rows.get(0).get(1));
    }

    @Test
    public void testSelectAllEmpty() {
        List<List<Object>> rows = db.selectAll("nonexistent");
        Assert.assertTrue(rows.isEmpty());
    }

    @Test
    public void testDeleteWithCondition() {
        db.createTable("items", "id", "value");
        db.insert("items", new Object[]{1, "a"});
        db.insert("items", new Object[]{2, "b"});
        db.insert("items", new Object[]{3, "a"});

        int removed = db.delete("items", "value", "a");
        Assert.assertEquals(2, removed);

        List<List<Object>> rows = db.selectAll("items");
        Assert.assertEquals(1, rows.size());
    }

    @Test
    public void testExecuteSelectQuery() {
        InMemoryDB.QueryResult result = db.executeQuery("SELECT * FROM travelrecord");
        Assert.assertNull(result.error);
        Assert.assertTrue(result.isSelect);
        Assert.assertNotNull(result.columns);
    }

    @Test
    public void testExecuteInsertQuery() {
        InMemoryDB.QueryResult result = db.executeQuery(
                "INSERT INTO travelrecord (id, name) VALUES (1, 'test')");
        Assert.assertNull(result.error);
        Assert.assertFalse(result.isSelect);
        Assert.assertEquals(1, result.affectedRows);
    }

    @Test
    public void testExecuteDeleteQuery() {
        db.insert("travelrecord", new Object[]{1, "toDelete"});
        InMemoryDB.QueryResult result = db.executeQuery(
                "DELETE FROM travelrecord WHERE name = 'toDelete'");
        Assert.assertNull(result.error);
        Assert.assertTrue(result.affectedRows > 0);
    }

    @Test
    public void testExecuteEmptyQuery() {
        InMemoryDB.QueryResult result = db.executeQuery("");
        Assert.assertNotNull(result.error);
    }

    @Test
    public void testExecuteSetCommand() {
        InMemoryDB.QueryResult result = db.executeQuery("SET autocommit=1");
        Assert.assertNull(result.error);
    }

    @Test
    public void testSelectLiteral() {
        InMemoryDB.QueryResult result = db.executeQuery("SELECT 'hello' AS greeting");
        Assert.assertNull(result.error);
        Assert.assertTrue(result.isSelect);
        Assert.assertEquals("greeting", result.columns.get(0));
        Assert.assertEquals("hello", result.rows.get(0).get(0));
    }
}