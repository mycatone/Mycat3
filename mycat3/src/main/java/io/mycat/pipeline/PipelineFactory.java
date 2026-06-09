package io.mycat.pipeline;

import io.mycat.engine.InMemoryDB;
import io.mycat.plugin.AuditPlugin;
import io.mycat.plugin.FirewallPlugin;
import io.mycat.plugin.InterceptorStage;
import io.mycat.plugin.SqlInterceptor;
import java.util.ArrayList;
import java.util.List;

public class PipelineFactory {

    public enum Mode {
        SIMPLE,
        WITH_AUDIT,
        WITH_FIREWALL,
        WITH_REWRITE,
        WITH_ROUTE,
        FULL,
        COMPLETE
    }

    public static Pipeline create(InMemoryDB db, Mode mode) {
        List<PipelineStage> stages = new ArrayList<>();
        stages.add(new ParseStage());

        List<SqlInterceptor> interceptors = new ArrayList<>();
        switch (mode) {
            case FULL:
            case COMPLETE:
                interceptors.add(new FirewallPlugin());
                interceptors.add(new AuditPlugin());
                break;
            case WITH_AUDIT:
                interceptors.add(new AuditPlugin());
                break;
            case WITH_FIREWALL:
                interceptors.add(new FirewallPlugin());
                break;
            case WITH_REWRITE:
            case WITH_ROUTE:
            case SIMPLE:
            default:
                break;
        }
        if (!interceptors.isEmpty()) {
            stages.add(new InterceptorStage(interceptors));
        }

        switch (mode) {
            case FULL:
            case COMPLETE:
                stages.add(new RewriteStage());
                stages.add(new RouteStage());
                stages.add(new OptimizeStage());
                break;
            case WITH_REWRITE:
                stages.add(new RewriteStage());
                break;
            case WITH_ROUTE:
                stages.add(new RouteStage());
                break;
            default:
                break;
        }

        stages.add(new ExecuteStage(db));
        return new DefaultPipeline(stages);
    }

    public static Pipeline createSimple(InMemoryDB db) {
        return create(db, Mode.SIMPLE);
    }

    public static Pipeline createFull(InMemoryDB db) {
        return create(db, Mode.FULL);
    }

    public static Pipeline createComplete(InMemoryDB db) {
        return create(db, Mode.COMPLETE);
    }

    public static Pipeline createWithRewrite(InMemoryDB db) {
        return create(db, Mode.WITH_REWRITE);
    }

    public static Pipeline createWithRoute(InMemoryDB db) {
        return create(db, Mode.WITH_ROUTE);
    }
}