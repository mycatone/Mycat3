package io.mycat.lifecycle;

public interface LifecycleComponent {
    String name();
    void initialize();
    void start();
    void stop();
}