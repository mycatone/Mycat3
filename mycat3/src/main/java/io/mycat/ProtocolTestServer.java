package io.mycat;

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
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ProtocolTestServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolTestServer.class);

    public static void main(String[] args) throws Exception {
        System.setProperty("vertx.logger-delegate-factory-class-name",
                "io.vertx.core.logging.SLF4JLogDelegateFactory");
        LOGGER.info("Starting Mycat2 Protocol Test Server...");

        ProtocolRegistry.register(new MySQLProtocolHandler());
        ProtocolRegistry.register(new TDSProtocolHandler());
        ProtocolRegistry.register(new TNSProtocolHandler());
        ProtocolRegistry.register(new PGProtocolHandler());

        Vertx vertx = Vertx.vertx();
        NetServer server = vertx.createNetServer(new NetServerOptions().setTcpNoDelay(true));

        server.connectHandler(socket -> {
            LOGGER.info("New connection from {}", socket.remoteAddress());

            final Buffer sniffBuf = Buffer.buffer();

            socket.handler(buf -> {
                sniffBuf.appendBuffer(buf);
                if (sniffBuf.length() >= 4) {
                    String protocol = ProtocolDetector.detect(sniffBuf);
                    if (protocol != null) {
                        LOGGER.info("Detected protocol: {}", protocol);
                        installHandler(socket, protocol, sniffBuf);
                    } else if (sniffBuf.length() > 10000) {
                        LOGGER.warn("Cannot detect protocol, closing");
                        socket.close();
                    }
                }
            });

            socket.closeHandler(v -> LOGGER.info("Connection closed: {}", socket.remoteAddress()));
            socket.exceptionHandler(t -> LOGGER.error("Socket error: {}", t.getMessage()));

            vertx.setTimer(100, id -> {
                if (sniffBuf.length() == 0) {
                    LOGGER.info("Timer: no data after 100ms, assuming MySQL");
                    installHandler(socket, "mysql", sniffBuf);
                }
            });
        });

        int port = 18066;
        server.listen(port, "0.0.0.0", result -> {
            if (result.succeeded()) {
                LOGGER.info("Protocol Test Server listening on port {}", port);
            } else {
                LOGGER.error("Failed to start server: {}", result.cause().getMessage());
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down server...");
            server.close(v -> vertx.close());
            latch.countDown();
        }));
        latch.await();
    }

    private static void installHandler(NetSocket socket, String protocol, Buffer sniffBuf) {
        LOGGER.info("Installing handler: protocol={}", protocol);

        ProtocolHandler handler = ProtocolRegistry.getHandler(protocol);
        if (handler != null) {
            handler.handle(socket, sniffBuf);
        } else {
            LOGGER.warn("No handler for protocol: {}", protocol);
            socket.close();
        }
    }
}