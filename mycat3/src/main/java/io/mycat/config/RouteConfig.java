package io.mycat.config;

import java.util.Map;

public class RouteConfig {
    private String match;
    private String shardKey;
    private String shardType;
    private String targetEndpoint;
    private Map<String, String> properties;

    public String getMatch() { return match; }
    public void setMatch(String match) { this.match = match; }
    public String getShardKey() { return shardKey; }
    public void setShardKey(String shardKey) { this.shardKey = shardKey; }
    public String getShardType() { return shardType; }
    public void setShardType(String shardType) { this.shardType = shardType; }
    public String getTargetEndpoint() { return targetEndpoint; }
    public void setTargetEndpoint(String targetEndpoint) { this.targetEndpoint = targetEndpoint; }
    public Map<String, String> getProperties() { return properties; }
    public void setProperties(Map<String, String> properties) { this.properties = properties; }
}