package io.mycat.config;

import java.util.Map;

public class YamlDataSourceConfig {
    private String name;
    private String type;
    private String jdbcUrl;
    private String username;
    private String password;
    private int minPoolSize = 2;
    private int maxPoolSize = 20;
    private Map<String, String> properties;

    public String getName() { return name; } public void setName(String name) { this.name = name; }
    public String getType() { return type; } public void setType(String type) { this.type = type; }
    public String getJdbcUrl() { return jdbcUrl; } public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }
    public String getUsername() { return username; } public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; } public void setPassword(String password) { this.password = password; }
    public int getMinPoolSize() { return minPoolSize; } public void setMinPoolSize(int minPoolSize) { this.minPoolSize = minPoolSize; }
    public int getMaxPoolSize() { return maxPoolSize; } public void setMaxPoolSize(int maxPoolSize) { this.maxPoolSize = maxPoolSize; }
    public Map<String, String> getProperties() { return properties; } public void setProperties(Map<String, String> properties) { this.properties = properties; }
}