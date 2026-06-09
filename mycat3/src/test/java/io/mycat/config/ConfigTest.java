package io.mycat.config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigTest {

    private RouteConfig routeConfig;
    private YamlDataSourceConfig dataSourceConfig;
    private YamlConfigLoader.ServerConfig serverConfig;

    @Before
    public void setUp() {
        routeConfig = new RouteConfig();
        dataSourceConfig = new YamlDataSourceConfig();
        serverConfig = new YamlConfigLoader.ServerConfig();
    }

    @Test
    public void testRouteConfigGettersSetters() {
        routeConfig.setMatch("/api/*");
        routeConfig.setShardKey("user_id");
        routeConfig.setShardType("hash");
        routeConfig.setTargetEndpoint("db-cluster-1");

        Map<String, String> props = new HashMap<>();
        props.put("timeout", "5000");
        routeConfig.setProperties(props);

        Assert.assertEquals("/api/*", routeConfig.getMatch());
        Assert.assertEquals("user_id", routeConfig.getShardKey());
        Assert.assertEquals("hash", routeConfig.getShardType());
        Assert.assertEquals("db-cluster-1", routeConfig.getTargetEndpoint());
        Assert.assertEquals("5000", routeConfig.getProperties().get("timeout"));
    }

    @Test
    public void testYamlDataSourceConfigGettersSetters() {
        dataSourceConfig.setName("master-ds");
        dataSourceConfig.setType("mysql");
        dataSourceConfig.setJdbcUrl("jdbc:mysql://localhost:3306/mydb");
        dataSourceConfig.setUsername("root");
        dataSourceConfig.setPassword("secret");
        dataSourceConfig.setMinPoolSize(5);
        dataSourceConfig.setMaxPoolSize(50);

        Map<String, String> props = new HashMap<>();
        props.put("autoCommit", "true");
        dataSourceConfig.setProperties(props);

        Assert.assertEquals("master-ds", dataSourceConfig.getName());
        Assert.assertEquals("mysql", dataSourceConfig.getType());
        Assert.assertEquals("jdbc:mysql://localhost:3306/mydb", dataSourceConfig.getJdbcUrl());
        Assert.assertEquals("root", dataSourceConfig.getUsername());
        Assert.assertEquals("secret", dataSourceConfig.getPassword());
        Assert.assertEquals(5, dataSourceConfig.getMinPoolSize());
        Assert.assertEquals(50, dataSourceConfig.getMaxPoolSize());
        Assert.assertEquals("true", dataSourceConfig.getProperties().get("autoCommit"));
    }

    @Test
    public void testYamlDataSourceConfigDefaults() {
        YamlDataSourceConfig defaultConfig = new YamlDataSourceConfig();
        Assert.assertEquals(2, defaultConfig.getMinPoolSize());
        Assert.assertEquals(20, defaultConfig.getMaxPoolSize());
    }

    @Test
    public void testYamlConfigLoaderLoad() {
        YamlConfigLoader loader = new YamlConfigLoader("server.yml");
        YamlConfigLoader.ServerConfig config = loader.load();
        Assert.assertEquals("mycat2-v2", config.getName());
        Assert.assertEquals(3306, config.getPort());
        Assert.assertEquals(16, config.getWorkerThreads());
    }

    @Test
    public void testServerConfigGettersSetters() {
        serverConfig.setName("test-server");
        serverConfig.setPort(8080);
        serverConfig.setWorkerThreads(32);

        List<RouteConfig> routes = new ArrayList<>();
        serverConfig.setRoutes(routes);
        List<YamlDataSourceConfig> dataSources = new ArrayList<>();
        serverConfig.setDataSources(dataSources);

        Assert.assertEquals("test-server", serverConfig.getName());
        Assert.assertEquals(8080, serverConfig.getPort());
        Assert.assertEquals(32, serverConfig.getWorkerThreads());
        Assert.assertNotNull(serverConfig.getRoutes());
        Assert.assertNotNull(serverConfig.getDataSources());
    }
}