package io.mycat.protocol.mysql;

import io.mycat.MySQLPacketUtil;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.packet.AuthPacket;
import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.DefaultFrontendSession;
import io.mycat.frontend.QueryResponse;
import io.mycat.pipeline.PipelineFactory;
import io.mycat.protocol.ProtocolHandler;
import io.mycat.vertx.ReadView;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MySQLProtocolHandler implements ProtocolHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLProtocolHandler.class);

    @Override
    public String getProtocolName() {
        return "mysql";
    }

    @Override
    public int getDefaultPort() {
        return 3306;
    }

    @Override
    public void handle(NetSocket socket, Buffer initialData) {
        new MySQLConnectionSession(socket).startHandshake(initialData);
    }

    private static class MySQLConnectionSession {
        private final NetSocket socket;
        private final InMemoryDB db;
        private DefaultFrontendSession frontendSession;
        private Buffer buffer;
        private boolean authenticated;
        private boolean processing;

        MySQLConnectionSession(NetSocket socket) {
            this.socket = socket;
            this.db = new InMemoryDB();
            this.buffer = Buffer.buffer();
            this.authenticated = false;
            this.processing = false;
        }

        void startHandshake(Buffer initialData) {
            LOGGER.info("Starting MySQL handshake for {}", socket.remoteAddress());

            byte[] seed = "abcdefghijklmnopqrst".getBytes();

            MySQLPayloadWriter writer = new MySQLPayloadWriter(1024);
            writer.writeByte(0x0a);
            writer.writeNULString("5.7.42-mycat2-protocol-test".getBytes());
            writer.writeFixInt(4, 1);
            writer.writeFixString(seed);
            writer.writeByte(0x00);
            writer.writeFixInt(2, 0xFFFF);
            writer.writeByte(0x21);
            writer.writeFixInt(2, 0x0002);
            writer.writeFixInt(2, 0x0020);
            writer.writeByte(seed.length + 1);
            writer.writeBytes(new byte[10]);
            writer.writeFixString(seed);
            writer.writeByte(0x00);
            writer.writeNULString("mysql_native_password".getBytes());

            byte[] generatedPacket = MySQLPacketUtil.generateMySQLPacket(0, writer);
            socket.write(Buffer.buffer(generatedPacket));
            LOGGER.info("Handshake sent, len={}", generatedPacket.length);

            socket.handler(new DispatchHandler());

            if (initialData != null && initialData.length() > 0) {
                buffer.appendBuffer(initialData);
            }
        }

        private class DispatchHandler implements Handler<Buffer> {
            @Override
            public void handle(Buffer event) {
                if (processing) return;
                processing = true;
                try {
                    buffer.appendBuffer(event);
                    while (buffer.length() > 3) {
                        int pktLen = readInt(buffer);
                        int totalLen = pktLen + 4;
                        if (buffer.length() < totalLen) break;
                        int seqId = buffer.getUnsignedByte(3);
                        Buffer payload = buffer.slice(4, totalLen);
                        byte[] remaining = buffer.getBytes(totalLen, buffer.length());
                        buffer = Buffer.buffer(remaining);

                        if (!authenticated) {
                            handleAuth(seqId, payload);
                        } else {
                            if (payload.length() > 0 && payload.getByte(0) == 0x03) {
                                handleQuery(seqId, payload);
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("MySQL handler error: {}", e.getMessage(), e);
                    try {
                        sendError(0, e.getMessage());
                    } catch (Exception ignored) {
                    }
                } finally {
                    processing = false;
                }
            }
        }

        private void handleAuth(int seqId, Buffer payload) {
            try {
                ReadView readView = new ReadView(payload);
                AuthPacket authPacket = new AuthPacket();
                authPacket.readPayload(readView);

                String username = authPacket.getUsername();
                String authPlugin = authPacket.getAuthPluginName();
                byte[] password = authPacket.getPassword();

                LOGGER.info("MySQL auth: user={}, plugin={}, passwordLen={}",
                        username, authPlugin, password != null ? password.length : 0);

                boolean pass = true;

                if (authPlugin != null && !authPlugin.isEmpty()
                        && !"mysql_native_password".equalsIgnoreCase(authPlugin)) {
                    pass = false;
                }

                if (pass) {
                    frontendSession = new DefaultFrontendSession(System.nanoTime(), db);
                    MySQLPayloadWriter ok = new MySQLPayloadWriter(32);
                    ok.writeByte(0x00);
                    ok.writeLenencInt(0);
                    ok.writeLenencInt(0);
                    ok.writeFixInt(2, 0x0002);
                    ok.writeFixInt(2, 0);
                    byte[] okPkt = MySQLPacketUtil.generateMySQLPacket(seqId + 1, ok);
                    socket.write(Buffer.buffer(okPkt));
                    authenticated = true;
                    LOGGER.info("MySQL auth OK for user={}", username);
                }
            } catch (Exception e) {
                LOGGER.error("Auth error: {}", e.getMessage(), e);
            }
        }

        private void handleQuery(int seqId, Buffer payload) {
            String sql = new String(payload.getBytes(1, payload.length()),
                    java.nio.charset.StandardCharsets.UTF_8);
            LOGGER.info("MySQL query: {}", sql);

            QueryResponse result;
            if (frontendSession != null) {
                result = frontendSession.executeSql(sql);
            } else {
                InMemoryDB.QueryResult dbResult = db.executeQuery(sql);
                result = convertResult(dbResult);
            }

            if (result.error() != null) {
                sendError(seqId, result.error());
            } else if (result.isSelect()) {
                sendResultSet(seqId, result);
            } else {
                sendOk(seqId, result.affectedRows());
            }
        }

        private QueryResponse convertResult(InMemoryDB.QueryResult dbResult) {
            if (dbResult.error != null) {
                return QueryResponse.error(dbResult.error);
            } else if (dbResult.isSelect) {
                return QueryResponse.resultSet(dbResult.columns, dbResult.rows);
            } else {
                return QueryResponse.ok(dbResult.affectedRows);
            }
        }

        private void sendResultSet(int seqId, QueryResponse result) {
            String[] columns = result.columns().toArray(new String[0]);
            int totalRows = result.rows().size();

            int s = seqId + 1;

            MySQLPayloadWriter colCount = new MySQLPayloadWriter(32);
            colCount.writeLenencInt(columns.length);
            socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(s++, colCount)));

            for (String colName : columns) {
                MySQLPayloadWriter col = new MySQLPayloadWriter(128);
                col.writeLenencString("def");
                col.writeLenencString("test");
                col.writeLenencString("test");
                col.writeLenencString(colName);
                col.writeLenencString(colName);
                col.writeFixInt(2, 0x0c);
                col.writeFixInt(4, 0x3F);
                col.writeByte(0x03);
                col.writeFixInt(2, 0x0000);
                col.writeByte(0x00);
                col.writeFixInt(2, 0x0000);
                socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(s++, col)));
            }

            MySQLPayloadWriter eof1 = new MySQLPayloadWriter(32);
            eof1.writeByte(0xFE);
            eof1.writeFixInt(2, 0);
            eof1.writeFixInt(2, 0x0002);
            socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(s++, eof1)));

            for (int i = 0; i < totalRows; i++) {
                List<Object> row = result.rows().get(i);
                MySQLPayloadWriter rw = new MySQLPayloadWriter(128);
                for (Object val : row) {
                    rw.writeLenencString(String.valueOf(val));
                }
                socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(s++, rw)));
            }

            MySQLPayloadWriter eof2 = new MySQLPayloadWriter(32);
            eof2.writeByte(0xFE);
            eof2.writeFixInt(2, 0);
            eof2.writeFixInt(2, 0x0002);
            socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(s++, eof2)));
        }

        private void sendOk(int seqId, long affectedRows) {
            MySQLPayloadWriter ok = new MySQLPayloadWriter(32);
            ok.writeByte(0x00);
            ok.writeLenencInt(affectedRows);
            ok.writeLenencInt(0);
            ok.writeFixInt(2, 0x0002);
            ok.writeFixInt(2, 0);
            socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(seqId + 1, ok)));
        }

        private void sendError(int seqId, String errorMsg) {
            MySQLPayloadWriter err = new MySQLPayloadWriter(128);
            err.writeByte(0xFF);
            err.writeFixInt(2, 1064);
            err.writeByte('#');
            err.writeNULString("42000".getBytes());
            err.writeNULString(errorMsg.getBytes());
            socket.write(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(seqId + 1, err)));
        }

        private int readInt(Buffer buffer) {
            return (buffer.getUnsignedByte(0))
                    | (buffer.getUnsignedByte(1) << 8)
                    | (buffer.getUnsignedByte(2) << 16);
        }
    }
}