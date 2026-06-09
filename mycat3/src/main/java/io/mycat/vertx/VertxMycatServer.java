/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.vertx;

import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.config.MycatServerConfig;
import io.mycat.monitor.LogEntryHolder;
import io.mycat.newquery.NewMycatConnectionConfig;
import io.mycat.runtime.MycatXaTranscation;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.*;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.protocol.ProtocolDetector;
import io.mycat.protocol.ProtocolHandler;
import io.mycat.protocol.ProtocolRegistry;
import io.mycat.protocol.api.MycatProtocolBackend;
import io.mycat.protocol.tds.TDSProtocolHandler;
import io.mycat.protocol.tns.TNSProtocolHandler;
import io.mycat.protocol.pg.PGProtocolHandler;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class VertxMycatServer implements MycatServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxMycatServer.class);
    MycatSessionManager server;
    private MycatServerConfig serverConfig;

    public VertxMycatServer(MycatServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public static void main(String[] args) {

    }

    @Override
    public RowBaseIterator showNativeDataSources() {
        return server.showNativeDataSources();
    }

    @Override
    public RowBaseIterator showConnections() {
        return server.showConnections();
    }

    @Override
    public RowBaseIterator showReactors() {
        return server.showReactors();
    }

    @Override
    public RowBaseIterator showBufferUsage(long sessionId) {
        return server.showBufferUsage(sessionId);
    }

    @Override
    public RowBaseIterator showNativeBackends() {
        return server.showNativeBackends();
    }

    @Override
    public long countConnection() {
        return server.countConnection();
    }

    @Override
    public void start() throws Exception {
        this.server = new MycatSessionManager(serverConfig);
        this.server.start();
    }

    @Override
    public int kill(List<Long> ids) {
        return server.kill(ids);
    }

    @Override
    public void stopAcceptConnect() {
        this.server.stopAcceptConnect();
    }

    @Override
    public void resumeAcceptConnect() {
        this.server.resumeAcceptConnect();
    }

    @Override
    public void setReadyToCloseSQL(String sql) {
        this.server.setReadyToCloseSQL(sql);
    }

    @Override
    public Future<Void> pause(List<Long> currentIds) {
        return this.server.pause(currentIds);
    }

    @Override
    public void resume() {
        this.server.resume();
    }

    @Override
    public boolean isPause() {
        return this.server.isPause();
    }

    public static class MycatSessionManager implements MycatServer {
        private final ConcurrentLinkedDeque<VertxSession> sessions = new ConcurrentLinkedDeque<>();
        private MycatServerConfig serverConfig;
        public boolean acceptConnect = true;
        public boolean pause = false;

        public MycatSessionManager(MycatServerConfig serverConfig) {
            this.serverConfig = serverConfig;
        }

        private final MycatProtocolBackend protocolBackend = new MycatProtocolBackend();

        @Override
        public void start() throws Exception {
            registerProtocol(new TDSProtocolHandler());
            registerProtocol(new TNSProtocolHandler());
            registerProtocol(new PGProtocolHandler());
            LOGGER.info("Multi-protocol support registered: mysql, sqlserver, oracle, postgresql");

            Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
            NetServerOptions netServerOptions = new NetServerOptions();
            netServerOptions.setReusePort(true);
            netServerOptions.setReuseAddress(true);
            netServerOptions.setUseProxyProtocol(serverConfig.getServer().isUseProxyProtocol());
            netServerOptions.setSendBufferSize(serverConfig.getServer().getSendBufferSize());
            netServerOptions.setReceiveBufferSize(serverConfig.getServer().getReceiveBufferSize());
            DeploymentOptions deploymentOptions = new DeploymentOptions();
            deploymentOptions.setWorker(false);
            boolean supportClientDeprecateEof = NewMycatConnectionConfig.CLIENT_DEPRECATE_EOF;
            int defaultServerCapabilities = MySQLServerCapabilityFlags.getDefaultServerCapabilities();
            if (supportClientDeprecateEof) {
                defaultServerCapabilities |= MySQLServerCapabilityFlags.CLIENT_DEPRECATE_EOF;
            } else {
                defaultServerCapabilities &= (~MySQLServerCapabilityFlags.CLIENT_DEPRECATE_EOF);
            }
            int finalServerCapabilities = defaultServerCapabilities;
            for (int i = 0; i < serverConfig.getServer().getReactorNumber(); i++) {
                vertx.deployVerticle(new AbstractVerticle() {
                    @Override
                    public void start() throws Exception {
                        NetServer netServer = vertx.createNetServer(netServerOptions);
                        netServer.connectHandler(socket -> {
                            if (!MycatSessionManager.this.acceptConnect) {
                                socket.close();
                                return;
                            }
                            socket.handler(new Handler<Buffer>() {
                                final Buffer sniffBuf = Buffer.buffer();
                                boolean detected = false;

                                {
                                    // T1 = 300ms: only treat as MySQL if the client has not sent
                                    // anything (MySQL is server-speaks-first; the other three
                                    // protocols speak first and would already have arrived).
                                    vertx.setTimer(300, id -> {
                                        if (!detected && sniffBuf.length() == 0) {
                                            detected = true;
                                            installHandler(socket, "mysql", sniffBuf, finalServerCapabilities);
                                        }
                                    });
                                    // T2 = 2000ms: hard ceiling. If the client sent bytes we
                                    // still couldn't classify, close — never silently route to
                                    // MySQL (that's what produced the old TDS/TNS/PG race).
                                    vertx.setTimer(2000, id -> {
                                        if (!detected) {
                                            detected = true;
                                            LOGGER.warn("Protocol undetected after 2s for {} (sniff={} bytes), closing",
                                                    socket.remoteAddress(), sniffBuf.length());
                                            socket.close();
                                        }
                                    });
                                }

                                @Override
                                public void handle(Buffer buf) {
                                    if (detected) return;
                                    sniffBuf.appendBuffer(buf);
                                    if (sniffBuf.length() >= 4) {
                                        String protocol = ProtocolDetector.detect(sniffBuf);
                                        if (protocol != null) {
                                            detected = true;
                                            installHandler(socket, protocol, sniffBuf, finalServerCapabilities);
                                        } else if (sniffBuf.length() > 2048) {
                                            LOGGER.warn("Cannot detect protocol for {} after 2KB, closing", socket.remoteAddress());
                                            socket.close();
                                        }
                                    }
                                }
                            });
                        }).listen(serverConfig.getServer().getPort(),
                                serverConfig.getServer().getIp(), listenResult -> {
                                    if (listenResult.succeeded()) {
                                        LOGGER.info("Mycat Vertx server " + super.deploymentID() +
                                                " started up (multi-protocol).");
                                    } else {
                                        LOGGER.error("Mycat Vertx server exit. because: " + listenResult.cause().getMessage(), listenResult.cause());
                                    }
                                });
                    }
                }, deploymentOptions);
            }

        }

        private void registerProtocol(ProtocolHandler handler) {
            handler.setBackend(protocolBackend);
            ProtocolRegistry.register(handler);
        }

        private void installHandler(NetSocket socket, String protocol, Buffer sniffBuf, int finalServerCapabilities) {
            LOGGER.info("Detected protocol: {} for {}", protocol, socket.remoteAddress());

            if ("mysql".equals(protocol)) {
                VertxMySQLAuthHandler authHandler = new VertxMySQLAuthHandler(socket, finalServerCapabilities, MycatSessionManager.this);
                authHandler.handle(sniffBuf);
            } else {
                ProtocolHandler handler = ProtocolRegistry.getHandler(protocol);
                if (handler != null) {
                    handler.handle(socket, sniffBuf);
                } else {
                    LOGGER.warn("No handler for protocol: {}", protocol);
                    socket.close();
                }
            }
        }

        @Override
        public int kill(List<Long> ids) {
            int count = 0;
            for (VertxSession session : sessions) {
                for (Long id : ids) {
                    if (session.getDataContext().getSessionId() == id) {
                        session.close(false, "kill");
                        count++;
                    }
                }
            }
            return count;
        }

        @Override
        public void stopAcceptConnect() {
            acceptConnect = false;
        }

        @Override
        public void resumeAcceptConnect() {
            acceptConnect = true;
        }

        @Override
        public void setReadyToCloseSQL(String sql) {
            for (VertxSession session : sessions) {
                session.getDataContext().setReadyToCloseSQL(sql);
            }
        }

        @Override
        public Future<Void> pause(List<Long> currentIds) {
            this.pause = true;
            return Future.future(promise -> Observable.interval(1, 1, TimeUnit.SECONDS).takeUntil(i -> {
                        return sessions.stream().filter(s -> !currentIds.contains(s.getDataContext().getSessionId())).allMatch(s -> s.isPause());
                    }).timeout(10, TimeUnit.SECONDS)
                    .subscribe(aLong -> promise.tryComplete(), throwable -> {
                        promise.tryFail(throwable);
                    }));
        }

        @Override
        public void resume() {
            this.pause = false;
            for (VertxSession session : this.sessions) {
                session.getSocket().resume();
            }
        }

        @Override
        public boolean isPause() {
            return this.pause;
        }

        public void addSession(VertxSession vertxSession) {
            NetSocket socket = vertxSession.getSocket();
            socket.closeHandler(event -> {
                LOGGER.info("session:{} is closing", vertxSession);
                sessions.remove(vertxSession);
            });
            sessions.add(vertxSession);
        }

        @Override
        public RowBaseIterator showNativeDataSources() {
            return demo();
        }


        @Override
        public RowBaseIterator showConnections() {
            List<MycatDataContext> sessions = MycatSessionManager.this.sessions.stream().map(i -> i.getDataContext()).collect(Collectors.toList());

            ResultSetBuilder builder = ResultSetBuilder.create();

            builder.addColumnInfo("ID", JDBCType.BIGINT);
            builder.addColumnInfo("USER_NAME", JDBCType.VARCHAR);
            builder.addColumnInfo("HOST", JDBCType.VARCHAR);
            builder.addColumnInfo("SCHEMA", JDBCType.VARCHAR);
            builder.addColumnInfo("AFFECTED_ROWS", JDBCType.BIGINT);
            builder.addColumnInfo("AUTOCOMMIT", JDBCType.VARCHAR);
            builder.addColumnInfo("IN_TRANSACTION", JDBCType.VARCHAR);
            builder.addColumnInfo("CHARSET", JDBCType.VARCHAR);
            builder.addColumnInfo("CHARSET_INDEX", JDBCType.BIGINT);
            builder.addColumnInfo("OPEN", JDBCType.VARCHAR);
            builder.addColumnInfo("SERVER_CAPABILITIES", JDBCType.BIGINT);
            builder.addColumnInfo("ISOLATION", JDBCType.VARCHAR);
            builder.addColumnInfo("LAST_ERROR_CODE", JDBCType.BIGINT);
            builder.addColumnInfo("LAST_INSERT_ID", JDBCType.BIGINT);
            builder.addColumnInfo("LAST_MESSAGE", JDBCType.VARCHAR);
            builder.addColumnInfo("PROCESS_STATE", JDBCType.VARCHAR);
            builder.addColumnInfo("WARNING_COUNT", JDBCType.BIGINT);
            builder.addColumnInfo("MYSQL_SESSION_ID", JDBCType.BIGINT);
            builder.addColumnInfo("TRANSACTION_TYPE", JDBCType.VARCHAR);
            builder.addColumnInfo("TRANSCATION_SNAPSHOT", JDBCType.VARCHAR);
            builder.addColumnInfo("CANCEL_FLAG", JDBCType.VARCHAR);
            builder.addColumnInfo("SQL", JDBCType.VARCHAR);

            for (MycatDataContext session : sessions) {
                long ID = session.getSessionId();
                MycatUser user = session.getUser();
                String USER_NAME = user.getUserName();
                String HOST = user.getHost();
                String SCHEMA = session.getDefaultSchema();
                long AFFECTED_ROWS = session.getAffectedRows();
                boolean AUTOCOMMIT = session.isAutocommit();
                boolean IN_TRANSACTION = session.isInTransaction();
                String CHARSET = Optional.ofNullable(session.getCharset()).map(i -> i.displayName()).orElse("");
                int CHARSET_INDEX = session.getCharsetIndex();
                boolean OPEN = true;
                int SERVER_CAPABILITIES = session.getServerCapabilities();
                String ISOLATION = session.getIsolation().getText();
                int LAST_ERROR_CODE = session.getLastErrorCode();
                long LAST_INSERT_ID = session.getLastInsertId();
                String LAST_MESSAGE = session.getLastMessage();
                String PROCESS_STATE = session.isRunning() ? "RUNNING" : "IDLE";

                int WARNING_COUNT = session.getWarningCount();
                Long MYSQL_SESSION_ID = ID;


                MycatDataContext dataContext = session;
                String TRANSACTION_TYPE = Optional.ofNullable(dataContext.transactionType()).map(i -> i.getName()).orElse("");

                MycatXaTranscation transactionSession = (MycatXaTranscation)dataContext.getTransactionSession();

                String TRANSCATION_SMAPSHOT = transactionSession.getAllConnections().toString();
                boolean CANCEL_FLAG = dataContext.getCancelFlag().get();

                LogEntryHolder holder = (LogEntryHolder) dataContext.getHolder();
                builder.addObjectRowPayload(Arrays.asList(
                        ID,
                        USER_NAME,
                        HOST,
                        SCHEMA,
                        AFFECTED_ROWS,
                        AUTOCOMMIT,
                        IN_TRANSACTION,
                        CHARSET,
                        CHARSET_INDEX,
                        OPEN,
                        SERVER_CAPABILITIES,
                        ISOLATION,
                        LAST_ERROR_CODE,
                        LAST_INSERT_ID,
                        LAST_MESSAGE,
                        PROCESS_STATE,
                        WARNING_COUNT,
                        MYSQL_SESSION_ID,
                        TRANSACTION_TYPE,
                        TRANSCATION_SMAPSHOT,
                        CANCEL_FLAG,
                        Optional.ofNullable(holder).map(i -> i.getSqlEntry()).map(i -> i.getSql()).orElse(null)
                ));
            }
            return builder.build();
        }

        @Override
        public RowBaseIterator showReactors() {
            return demo();
        }

        @Override
        public RowBaseIterator showBufferUsage(long sessionId) {
            return demo();
        }

        @Override
        public RowBaseIterator showNativeBackends() {
            return demo();
        }

        @Override
        public long countConnection() {
            return sessions.size();
        }
    }

    private static RowBaseIterator demo() {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("demo", JDBCType.VARCHAR);
        resultSetBuilder.addObjectRowPayload(Arrays.asList("unsupported"));
        return resultSetBuilder.build();
    }
}