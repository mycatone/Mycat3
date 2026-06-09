package io.mycat.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

public class LifecycleManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LifecycleManager.class);
    private final List<LifecycleComponent> components = new ArrayList<>();
    private LifecycleState state = LifecycleState.NEW;

    public enum LifecycleState { NEW, INITIALIZING, STARTED, STOPPING, STOPPED }

    public void register(LifecycleComponent component) {
        components.add(component);
    }

    public void start() {
        LOGGER.info("Starting lifecycle...");
        state = LifecycleState.INITIALIZING;
        for (LifecycleComponent component : components) {
            try {
                component.initialize();
                component.start();
            } catch (Exception e) {
                LOGGER.error("Failed to start component: {}", component.name(), e);
                throw new RuntimeException(e);
            }
        }
        state = LifecycleState.STARTED;
        LOGGER.info("Lifecycle started with {} components", components.size());
    }

    public void stop() {
        LOGGER.info("Stopping lifecycle...");
        state = LifecycleState.STOPPING;
        for (int i = components.size() - 1; i >= 0; i--) {
            try {
                components.get(i).stop();
            } catch (Exception e) {
                LOGGER.error("Failed to stop component: {}", components.get(i).name(), e);
            }
        }
        state = LifecycleState.STOPPED;
        LOGGER.info("Lifecycle stopped");
    }

    public LifecycleState getState() { return state; }
}