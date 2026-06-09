package io.mycat.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class PluginLoader {
    private final List<Plugin> plugins = new ArrayList<>();

    public PluginLoader() {
        loadPlugins();
    }

    private void loadPlugins() {
        ServiceLoader<Plugin> loader = ServiceLoader.load(Plugin.class);
        for (Plugin plugin : loader) {
            plugins.add(plugin);
        }
    }

    public List<Plugin> getPlugins() {
        return new ArrayList<>(plugins);
    }

    public void register(Plugin plugin) {
        plugins.add(plugin);
        plugin.init();
    }

    public void shutdown() {
        for (Plugin plugin : plugins) {
            plugin.destroy();
        }
    }
}