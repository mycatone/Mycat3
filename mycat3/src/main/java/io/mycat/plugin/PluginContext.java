package io.mycat.plugin;

import java.util.HashMap;
import java.util.Map;

public class PluginContext {
    private final Map<String, Object> attributes = new HashMap<>();

    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public Map<String, Object> getAttributes() {
        return new HashMap<>(attributes);
    }
}