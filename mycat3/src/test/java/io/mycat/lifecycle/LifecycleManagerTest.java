package io.mycat.lifecycle;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

public class LifecycleManagerTest {

    private LifecycleManager lifecycle;
    private List<String> orderLog;

    @Before
    public void setUp() {
        lifecycle = new LifecycleManager();
        orderLog = new ArrayList<>();
    }

    @Test
    public void testInitialStateNew() {
        Assert.assertEquals(LifecycleManager.LifecycleState.NEW, lifecycle.getState());
    }

    @Test
    public void testStartChangesStateToStarted() {
        lifecycle.start();
        Assert.assertEquals(LifecycleManager.LifecycleState.STARTED, lifecycle.getState());
    }

    @Test
    public void testStopChangesStateToStopped() {
        lifecycle.start();
        lifecycle.stop();
        Assert.assertEquals(LifecycleManager.LifecycleState.STOPPED, lifecycle.getState());
    }

    @Test
    public void testComponentOrder() {
        lifecycle.register(new TestComponent("A", orderLog));
        lifecycle.register(new TestComponent("B", orderLog));
        lifecycle.register(new TestComponent("C", orderLog));
        lifecycle.start();
        Assert.assertEquals("A.init", orderLog.get(0));
        Assert.assertEquals("A.start", orderLog.get(1));
        Assert.assertEquals("B.init", orderLog.get(2));
        Assert.assertEquals("B.start", orderLog.get(3));
        Assert.assertEquals("C.init", orderLog.get(4));
        Assert.assertEquals("C.start", orderLog.get(5));
        Assert.assertEquals(6, orderLog.size());
    }

    @Test
    public void testStopReverseOrder() {
        lifecycle.register(new TestComponent("A", orderLog));
        lifecycle.register(new TestComponent("B", orderLog));
        lifecycle.register(new TestComponent("C", orderLog));
        lifecycle.start();
        lifecycle.stop();
        int size = orderLog.size();
        Assert.assertEquals("C.stop", orderLog.get(size - 3));
        Assert.assertEquals("B.stop", orderLog.get(size - 2));
        Assert.assertEquals("A.stop", orderLog.get(size - 1));
    }

    @Test
    public void testEmptyLifecycle() {
        lifecycle.start();
        Assert.assertEquals(LifecycleManager.LifecycleState.STARTED, lifecycle.getState());
        lifecycle.stop();
        Assert.assertEquals(LifecycleManager.LifecycleState.STOPPED, lifecycle.getState());
    }

    private static class TestComponent implements LifecycleComponent {
        private final String name;
        private final List<String> log;

        TestComponent(String name, List<String> log) {
            this.name = name;
            this.log = log;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void initialize() {
            log.add(name + ".init");
        }

        @Override
        public void start() {
            log.add(name + ".start");
        }

        @Override
        public void stop() {
            log.add(name + ".stop");
        }
    }
}