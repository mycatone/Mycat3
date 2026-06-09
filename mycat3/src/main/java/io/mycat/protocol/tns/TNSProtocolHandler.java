package io.mycat.protocol.tns;

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

/**
 * TNS (Oracle Net Services) front-end. Completes Connect/Accept then funnels
 * Data payloads through the shared {@link MycatProtocolBackend}.
 *
 * Auth disclaimer: Oracle's real NS authentication is a multi-round O5LOGON
 * exchange (AUTH_SESSKEY ↔ AUTH_PASSWORD with AES-encrypted challenges, RFCs
 * are not public). Implementing it server-side without the Oracle Net SDK is
 * a multi-week project on its own. As a documented limitation, this handler
 * uses a fixed username ({@link #DEFAULT_USER}) resolved via the standard
 * {@link io.mycat.Authenticator} and accepts any client that survives
 * Connect/Accept. See Mycat3配置说明.md §1.1.
 */
public class TNSProtocolHandler implements ProtocolHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TNSProtocolHandler.class);

    private static final String DEFAULT_USER = "root";

    private static final int TNS_CONNECT = 1;
    private static final int TNS_ACCEPT = 2;
    private static final int TNS_DATA = 6;
    private static final int TNS_RESEND = 11;
    private static final int TNS_MARKER = 12;
    private static final int TNS_ATTENTION = 13;

    private enum State { CONNECT, READY, CLOSED }

    private volatile MycatProtocolBackend backend;

    @Override
    public String getProtocolName() {
        return "oracle";
    }

    @Override
    public int getDefaultPort() {
        return 1521;
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
        private State state = State.CONNECT;
        private Buffer buffer = Buffer.buffer();
        private MycatDataContext ctx;

        ConnectionSession(MycatProtocolBackend backend) {
            this.backend = backend;
        }

        private void dispatch(NetSocket socket, Buffer data) {
            try {
                buffer.appendBuffer(data);
                byte[] bytes = buffer.getBytes();
                if (bytes.length < 8) return;

                int totalLength = ((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF);
                if (bytes.length < totalLength) return;

                int packetType = bytes[4] & 0xFF;

                if (packetType == TNS_CONNECT && state == State.CONNECT) {
                    handleConnect(socket);
                } else if (packetType == TNS_DATA && state == State.READY) {
                    handleData(socket, bytes, totalLength);
                } else if (packetType == TNS_MARKER) {
                    LOGGER.debug("TNS marker received");
                } else if (packetType == TNS_ATTENTION) {
                    LOGGER.debug("TNS attention/break received");
                } else {
                    LOGGER.debug("TNS unknown packet type: {}", packetType);
                }
                byte[] remaining = new byte[buffer.length() - totalLength];
                if (remaining.length > 0) {
                    System.arraycopy(bytes, totalLength, remaining, 0, remaining.length);
                }
                buffer = Buffer.buffer(remaining);
            } catch (Exception e) {
                LOGGER.error("TNS handler error", e);
                socket.close();
            }
        }

        private void handleConnect(NetSocket socket) {
            LOGGER.info("TNS Connect received");

            if (backend == null) {
                LOGGER.error("TNS: no backend wired; closing");
                socket.close();
                return;
            }
            String host = remoteHost(socket);
            UserConfig userInfo = backend.lookupUserConfig(DEFAULT_USER);
            if (userInfo == null) {
                LOGGER.warn("TNS: {} not configured; closing", DEFAULT_USER);
                socket.close();
                return;
            }
            this.ctx = backend.createContext(DEFAULT_USER, host,
                    new InetSocketAddress(socket.remoteAddress().host(), socket.remoteAddress().port()),
                    userInfo, null);

            byte[] response = concat(buildAcceptPacket(), buildResend());
            socket.write(Buffer.buffer(response));
            state = State.READY;
            LOGGER.info("TNS Accept sent (auth simplified — fixed user '{}')", DEFAULT_USER);
        }

        private void handleData(NetSocket socket, byte[] bytes, int totalLength) {
            int payloadLen = totalLength - 8;
            if (payloadLen <= 0) return;
            String data = new String(bytes, 8, payloadLen, StandardCharsets.UTF_8).trim();
            if (data.isEmpty()) return;
            LOGGER.info("TNS Data: {}", data);

            backend.executeSql(data, ctx, size -> new TNSResponse(socket, ctx, size))
                    .onFailure(t -> {
                        LOGGER.error("TNS SQL failed: {}", data, t);
                        new TNSResponse(socket, ctx, 1).sendError(t);
                    });
        }
    }

    private static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) total += p.length;
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }

    private static String remoteHost(NetSocket socket) {
        try {
            return socket.remoteAddress().host();
        } catch (Throwable ignored) {
            return "";
        }
    }

    private static byte[] buildAcceptPacket() {
        int bodyLen = 32;
        byte[] buf = new byte[8 + bodyLen];
        buf[0] = 0x00; buf[1] = (byte) (8 + bodyLen); buf[2] = 0x00; buf[3] = 0x00;
        buf[4] = TNS_ACCEPT;
        buf[5] = 0x00; buf[6] = 0x00; buf[7] = 0x00;
        buf[8] = 0x00; buf[9] = 0x06;
        buf[10] = 0x0C; buf[11] = 0x0C;
        buf[12] = 0x00; buf[13] = 0x00; buf[14] = 0x00; buf[15] = 0x00;
        buf[16] = 0x0C; buf[17] = 0x01;
        buf[18] = 0x01; buf[19] = 0x00;
        buf[20] = 0x00; buf[21] = 0x00; buf[22] = 0x00; buf[23] = 0x00;
        return buf;
    }

    private static byte[] buildResend() {
        byte[] buf = new byte[10];
        buf[0] = 0x00; buf[1] = (byte) buf.length; buf[2] = 0x00; buf[3] = 0x00;
        buf[4] = TNS_RESEND;
        buf[5] = 0x00; buf[6] = 0x00; buf[7] = 0x00;
        buf[8] = 0x00; buf[9] = 0x00;
        return buf;
    }
}
