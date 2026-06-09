package io.mycat.protocol.pg;

import io.mycat.MycatDataContext;
import io.mycat.config.UserConfig;
import io.mycat.protocol.ProtocolHandler;
import io.mycat.protocol.api.MycatProtocolBackend;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;

public class PGProtocolHandler implements ProtocolHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PGProtocolHandler.class);

    private static final int SSL_REQUEST_CODE = 80877103;
    private static final int PROTOCOL_VERSION_30 = 196608;
    private static final SecureRandom RNG = new SecureRandom();

    private enum State { STARTUP, SSL_NEGOTIATION, AWAIT_PASSWORD, READY, CLOSED }

    private volatile MycatProtocolBackend backend;

    @Override
    public String getProtocolName() {
        return "postgresql";
    }

    @Override
    public int getDefaultPort() {
        return 5432;
    }

    @Override
    public void setBackend(MycatProtocolBackend backend) {
        this.backend = backend;
    }

    @Override
    public void handle(NetSocket socket, Buffer initialData) {
        ConnectionSession session = new ConnectionSession(backend);
        socket.handler(buffer -> session.dispatch(socket, buffer));
        if (initialData != null && initialData.length() > 0) {
            session.dispatch(socket, initialData);
        }
    }

    private static class ConnectionSession {
        private final MycatProtocolBackend backend;
        private State state = State.STARTUP;
        private Buffer buffer = Buffer.buffer();
        private MycatDataContext ctx;
        private String pendingUsername;
        private String pendingDatabase;
        private byte[] md5Salt;

        ConnectionSession(MycatProtocolBackend backend) {
            this.backend = backend;
        }

        private void dispatch(NetSocket socket, Buffer data) {
            try {
                buffer.appendBuffer(data);
                if (buffer.length() < 4) return;

                byte[] bytes = buffer.getBytes();

                if (state == State.STARTUP || state == State.SSL_NEGOTIATION) {
                    int msgLength = readInt32(bytes, 0);
                    if (msgLength > bytes.length) return;

                    int protocolVersion = readInt32(bytes, 4);
                    if (protocolVersion == SSL_REQUEST_CODE) {
                        socket.write(Buffer.buffer(new byte[]{'N'}));
                        state = State.SSL_NEGOTIATION;
                        buffer = copyRemaining(buffer, msgLength);
                        return;
                    }
                    if (protocolVersion == PROTOCOL_VERSION_30) {
                        Map<String, String> params = parseStartupParams(bytes, msgLength);
                        pendingUsername = params.getOrDefault("user", "");
                        pendingDatabase = params.getOrDefault("database", "");
                        LOGGER.info("PG Startup: user={}, database={}", pendingUsername, pendingDatabase);

                        if (backend == null) {
                            LOGGER.error("PG: no backend wired; closing");
                            socket.close();
                            return;
                        }
                        String host = remoteHost(socket);
                        String storedPassword = backend.lookupStoredPassword(pendingUsername, host);
                        if (storedPassword == null) {
                            sendAuthFailure(socket, "authentication failed for user \"" + pendingUsername + "\"");
                            return;
                        }
                        if (storedPassword.isEmpty()) {
                            completeAuth(socket, host);
                            buffer = copyRemaining(buffer, msgLength);
                            return;
                        }
                        // Issue MD5 challenge.
                        md5Salt = new byte[4];
                        RNG.nextBytes(md5Salt);
                        socket.write(Buffer.buffer(buildAuthMd5(md5Salt)));
                        state = State.AWAIT_PASSWORD;
                        buffer = copyRemaining(buffer, msgLength);
                        return;
                    }
                    buffer = Buffer.buffer();
                    return;
                }

                if (state == State.AWAIT_PASSWORD) {
                    if (bytes.length < 5) return;
                    byte msgType = bytes[0];
                    int msgBodyLen = readInt32(bytes, 1);
                    int totalLen = 1 + msgBodyLen;
                    if (totalLen > bytes.length) return;
                    if (msgType != 'p') {
                        sendAuthFailure(socket, "expected PasswordMessage");
                        return;
                    }
                    int payloadLen = msgBodyLen - 4 - 1; // -length field, -trailing null
                    if (payloadLen < 0) payloadLen = 0;
                    String clientHash = new String(bytes, 5, payloadLen, StandardCharsets.UTF_8);
                    String storedPassword = backend.lookupStoredPassword(pendingUsername, remoteHost(socket));
                    if (storedPassword == null) {
                        sendAuthFailure(socket, "authentication failed for user \"" + pendingUsername + "\"");
                        return;
                    }
                    String expected = computePgMd5(storedPassword, pendingUsername, md5Salt);
                    if (!expected.equals(clientHash)) {
                        sendAuthFailure(socket, "password authentication failed for user \"" + pendingUsername + "\"");
                        return;
                    }
                    completeAuth(socket, remoteHost(socket));
                    buffer = copyRemaining(buffer, totalLen);
                    return;
                }

                if (state == State.READY) {
                    if (bytes.length < 5) return;
                    byte msgType = bytes[0];
                    int msgBodyLen = readInt32(bytes, 1);
                    if (msgBodyLen < 4) {
                        buffer = Buffer.buffer();
                        return;
                    }
                    int totalLen = 1 + msgBodyLen;
                    if (totalLen > bytes.length) return;

                    switch (msgType) {
                        case 'Q':
                            handleQuery(socket, bytes, msgBodyLen);
                            break;
                        case 'P':
                            socket.write(Buffer.buffer(PGResponse.buildCommandComplete("PARSE")));
                            break;
                        case 'B':
                            socket.write(Buffer.buffer(PGResponse.buildCommandComplete("BIND")));
                            break;
                        case 'D':
                            socket.write(Buffer.buffer(PGResponse.buildCommandComplete("DESCRIBE")));
                            break;
                        case 'E':
                            socket.write(Buffer.buffer(PGResponse.buildCommandComplete("SELECT 0")));
                            break;
                        case 'S':
                            socket.write(Buffer.buffer(PGResponse.buildReadyForQuery((byte) 'I')));
                            break;
                        case 'H':
                        case 'C':
                            break;
                        case 'X':
                            state = State.CLOSED;
                            socket.close();
                            return;
                        default:
                            LOGGER.debug("PG unknown message type: {}", (char) msgType);
                            break;
                    }
                    buffer = copyRemaining(buffer, totalLen);
                }
            } catch (Exception e) {
                LOGGER.error("PG handler error", e);
                socket.close();
            }
        }

        private void completeAuth(NetSocket socket, String host) {
            UserConfig userInfo = backend.lookupUserConfig(pendingUsername);
            if (userInfo == null) {
                sendAuthFailure(socket, "authentication failed for user \"" + pendingUsername + "\"");
                return;
            }
            this.ctx = backend.createContext(pendingUsername, host,
                    new InetSocketAddress(socket.remoteAddress().host(), socket.remoteAddress().port()),
                    userInfo, pendingDatabase);

            Buffer response = Buffer.buffer();
            response.appendBytes(buildAuthOk());
            response.appendBytes(buildParameterStatus("server_version", "14.0"));
            response.appendBytes(buildParameterStatus("server_encoding", "UTF8"));
            response.appendBytes(buildParameterStatus("client_encoding", "UTF8"));
            response.appendBytes(buildParameterStatus("DateStyle", "ISO, MDY"));
            response.appendBytes(buildParameterStatus("integer_datetimes", "on"));
            response.appendBytes(buildBackendKeyData(42, 12345));
            response.appendBytes(PGResponse.buildReadyForQuery((byte) 'I'));
            socket.write(response);
            state = State.READY;
        }

        private void sendAuthFailure(NetSocket socket, String message) {
            Buffer err = Buffer.buffer();
            err.appendBytes(PGResponse.buildErrorResponse(message));
            socket.write(err);
            state = State.CLOSED;
            socket.close();
        }

        private void handleQuery(NetSocket socket, byte[] bytes, int msgLength) {
            int sqlLen = msgLength - 4;
            if (sqlLen > 0 && bytes[5 + sqlLen - 1] == 0) sqlLen--;
            String sql = new String(bytes, 5, sqlLen, StandardCharsets.UTF_8).trim();
            LOGGER.info("PG query: {}", sql);

            String hint = sql;
            backend.executeSql(sql, ctx, size -> new PGResponse(socket, ctx, size, hint))
                    .onFailure(t -> {
                        LOGGER.error("PG SQL failed: {}", sql, t);
                        Buffer err = Buffer.buffer();
                        err.appendBytes(PGResponse.buildErrorResponse(t.getMessage() == null ? "execution error" : t.getMessage()));
                        err.appendBytes(PGResponse.buildReadyForQuery((byte) 'I'));
                        socket.write(err);
                    });
        }
    }

    private static String remoteHost(NetSocket socket) {
        try {
            return socket.remoteAddress().host();
        } catch (Throwable ignored) {
            return "";
        }
    }

    private static Map<String, String> parseStartupParams(byte[] bytes, int length) {
        Map<String, String> params = new LinkedHashMap<>();
        int pos = 8;
        while (pos < length) {
            int end = indexOfNull(bytes, pos);
            if (end < 0) break;
            String key = new String(bytes, pos, end - pos, StandardCharsets.UTF_8);
            pos = end + 1;
            end = indexOfNull(bytes, pos);
            if (end < 0) break;
            String value = new String(bytes, pos, end - pos, StandardCharsets.UTF_8);
            pos = end + 1;
            params.put(key, value);
        }
        return params;
    }

    private static int indexOfNull(byte[] bytes, int start) {
        for (int i = start; i < bytes.length; i++) {
            if (bytes[i] == 0) return i;
        }
        return -1;
    }

    private static byte[] buildAuthOk() {
        byte[] buf = new byte[9];
        buf[0] = 'R';
        System.arraycopy(PGResponse.intToBytes(8), 0, buf, 1, 4);
        System.arraycopy(PGResponse.intToBytes(0), 0, buf, 5, 4);
        return buf;
    }

    private static byte[] buildAuthMd5(byte[] salt) {
        byte[] buf = new byte[13];
        buf[0] = 'R';
        System.arraycopy(PGResponse.intToBytes(12), 0, buf, 1, 4);
        System.arraycopy(PGResponse.intToBytes(5), 0, buf, 5, 4);
        System.arraycopy(salt, 0, buf, 9, 4);
        return buf;
    }

    private static byte[] buildParameterStatus(String key, String value) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
        int len = 4 + keyBytes.length + 1 + valBytes.length + 1;
        byte[] buf = new byte[1 + len];
        buf[0] = 'S';
        System.arraycopy(PGResponse.intToBytes(len), 0, buf, 1, 4);
        int pos = 5;
        System.arraycopy(keyBytes, 0, buf, pos, keyBytes.length);
        pos += keyBytes.length;
        buf[pos++] = 0;
        System.arraycopy(valBytes, 0, buf, pos, valBytes.length);
        pos += valBytes.length;
        buf[pos] = 0;
        return buf;
    }

    private static byte[] buildBackendKeyData(int pid, int secret) {
        byte[] buf = new byte[13];
        buf[0] = 'K';
        System.arraycopy(PGResponse.intToBytes(12), 0, buf, 1, 4);
        System.arraycopy(PGResponse.intToBytes(pid), 0, buf, 5, 4);
        System.arraycopy(PGResponse.intToBytes(secret), 0, buf, 9, 4);
        return buf;
    }

    private static int readInt32(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    private static Buffer copyRemaining(Buffer src, int from) {
        if (src.length() > from) {
            byte[] remaining = src.getBytes(from, src.length());
            return Buffer.buffer(remaining);
        }
        return Buffer.buffer();
    }

    private static String computePgMd5(String password, String username, byte[] salt) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update((password + username).getBytes(StandardCharsets.UTF_8));
            String innerHex = toHex(md.digest());
            md.reset();
            md.update(innerHex.getBytes(StandardCharsets.US_ASCII));
            md.update(salt);
            return "md5" + toHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 not available", e);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
