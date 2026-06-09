package io.mycat.server;

import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.SessionFactory;
import io.mycat.frontend.SessionManager;
import io.mycat.lifecycle.LifecycleComponent;
import io.mycat.monitor.HealthEndpoint;
import io.mycat.monitor.MetricsCollector;
import io.mycat.protocol.ProtocolDetector;
import io.mycat.protocol.ProtocolHandler;
import io.mycat.protocol.ProtocolRegistry;
import io.mycat.protocol.mysql.MySQLProtocolHandler;
import io.mycat.protocol.pg.PGProtocolHandler;
import io.mycat.protocol.tds.TDSProtocolHandler;
import io.mycat.protocol.tns.TNSProtocolHandler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MycatServer implements LifecycleComponent {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatServer.class);

    private final Vertx vertx;
    private final NetServer netServer;
    private final int port;
    private final InMemoryDB db;
    private final SessionManager sessionManager;
    private final SessionFactory sessionFactory;
    private final MetricsCollector metrics;
    private final HealthEndpoint healthEndpoint;

    public MycatServer(Vertx vertx, int port) {
        this.vertx = vertx;
        this.port = port;
        this.db = new InMemoryDB();
        this.sessionManager = new SessionManager(1000, 3600000);
        this.sessionFactory = new SessionFactory(db);
        this.metrics = MetricsCollector.getInstance();
        this.healthEndpoint = new HealthEndpoint();
        this.netServer = vertx.createNetServer(new NetServerOptions().setPort(port));
    }

    @Override public String name() { return "mycat-server"; }

    @Override
    public void initialize() {
        LOGGER.info("Initializing MycatServer on port {}", port);
        ProtocolRegistry.register(new MySQLProtocolHandler());
        ProtocolRegistry.register(new TDSProtocolHandler());
        ProtocolRegistry.register(new TNSProtocolHandler());
        ProtocolRegistry.register(new PGProtocolHandler());
    }

    @Override
    public void start() {
        netServer.connectHandler(socket -> {
            LOGGER.info("New connection: {}", socket.remoteAddress());
            final Buffer sniffBuf = Buffer.buffer();

            socket.handler(buf -> {
                sniffBuf.appendBuffer(buf);
                if (sniffBuf.length() >= 4) {
                    String protocol = ProtocolDetector.detect(sniffBuf);
                    if (protocol != null) {
                        ProtocolHandler handler = ProtocolRegistry.getHandler(protocol);
                        if (handler != null) {
                            handler.handle(socket, sniffBuf);
                            metrics.recordQuery(protocol, "connect");
                        }
                    }
                }
            });

            socket.closeHandler(v -> LOGGER.info("Connection closed: {}", socket.remoteAddress()));
            socket.exceptionHandler(t -> LOGGER.error("Socket error: {}", t.getMessage()));
        });

        netServer.listen(res -> {
            if (res.succeeded()) {
                LOGGER.info("MycatServer listening on port {}", port);
            } else {
                LOGGER.error("Failed to start server", res.cause());
            }
        });
    }

    @Override
    public void stop() {
        sessionManager.shutdown();
        netServer.close();
    }

    public int getPort() { return port; }
    public SessionManager getSessionManager() { return sessionManager; }
    public HealthEndpoint getHealthEndpoint() { return healthEndpoint; }
}