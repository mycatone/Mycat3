package io.mycat.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class YamlConfigLoader {
    private String configPath;

    public YamlConfigLoader(String configPath) {
        this.configPath = configPath;
    }

    public ServerConfig load() {
        ServerConfig config = new ServerConfig();
        config.setName("mycat2-v2");
        config.setPort(3306);
        config.setRoutes(new ArrayList<>());
        config.setDataSources(new ArrayList<>());
        return config;
    }

    public static class ServerConfig {
        private String name;
        private int port;
        private List<RouteConfig> routes;
        private List<YamlDataSourceConfig> dataSources;
        private int workerThreads = 16;

        public String getName() { return name; } public void setName(String name) { this.name = name; }
        public int getPort() { return port; } public void setPort(int port) { this.port = port; }
        public List<RouteConfig> getRoutes() { return routes; } public void setRoutes(List<RouteConfig> routes) { this.routes = routes; }
        public List<YamlDataSourceConfig> getDataSources() { return dataSources; } public void setDataSources(List<YamlDataSourceConfig> dataSources) { this.dataSources = dataSources; }
        public int getWorkerThreads() { return workerThreads; } public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    }
}