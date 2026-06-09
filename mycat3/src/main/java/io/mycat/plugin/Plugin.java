package io.mycat.plugin;

public interface Plugin {
    String name();
    String version();
    default void init() {}
    default void destroy() {}
}