package io.mycat.protocol.tds;

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

public class TDSProtocolHandler implements ProtocolHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TDSProtocolHandler.class);

    private static final byte TDS_PRELOGIN = 0x12;
    private static final byte TDS_LOGIN7 = 0x10;
    private static final byte TDS_SQL_BATCH = 0x01;
    private static final byte TDS_RPC_REQUEST = 0x03;
    private static final byte TDS_TABULAR_RESULT = 0x04;
    private static final byte TDS_ATTENTION = 0x06;

    /** PreLogin token TYPE_ENCRYPTION value: encryption not supported by server. */
    private static final byte ENCRYPT_NOT_SUP = 0x02;

    private static final byte TOKEN_LOGINACK = (byte) 0xAD;
    private static final byte TOKEN_ENVCHANGE = (byte) 0xE3;
    private static final byte TOKEN_INFO = (byte) 0xAB;

    // TDS protocol version constants (big-endian DWORD as seen on the wire).
    private static final int TDS_VERSION_70 = 0x00000070; // SQL Server 7.0
    private static final int TDS_VERSION_71 = 0x01000071; // SQL Server 2000
    private static final int TDS_VERSION_72 = 0x02000972; // SQL Server 2005
    private static final int TDS_VERSION_73A = 0x03000A73; // SQL Server 2008
    private static final int TDS_VERSION_73B = 0x03000B73; // SQL Server 2008 R2
    private static final int TDS_VERSION_74 = 0x04000074; // SQL Server 2012+

    private enum State { PRELOGIN, LOGIN, READY, CLOSED }

    private volatile MycatProtocolBackend backend;

    @Override
    public String getProtocolName() {
        return "sqlserver";
    }

    @Override
    public int getDefaultPort() {
        return 1433;
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
        private State state = State.PRELOGIN;
        private Buffer buffer = Buffer.buffer();
        private MycatDataContext ctx;
        private String database = "master";

        ConnectionSession(MycatProtocolBackend backend) {
            this.backend = backend;
        }

        private void dispatch(NetSocket socket, Buffer data) {
            try {
                buffer.appendBuffer(data);
                byte[] bytes = buffer.getBytes();
                if (bytes.length < 8) return;

                // TDS header bytes 2-3 = total packet length, big-endian.
                int totalLength = ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
                if (bytes.length < totalLength) return;

                int packetType = bytes[0] & 0xFF;

                if (packetType == TDS_PRELOGIN && state == State.PRELOGIN) {
                    handlePreLogin(socket);
                } else if (packetType == TDS_LOGIN7 && state == State.LOGIN) {
                    handleLogin7(socket, bytes, totalLength);
                } else if (packetType == TDS_SQL_BATCH || packetType == TDS_RPC_REQUEST) {
                    handleSqlBatch(socket, bytes, totalLength, packetType);
                } else if (packetType == TDS_ATTENTION) {
                    LOGGER.debug("TDS attention received");
                }
                byte[] remaining = new byte[buffer.length() - totalLength];
                if (remaining.length > 0) {
                    System.arraycopy(bytes, totalLength, remaining, 0, remaining.length);
                }
                buffer = Buffer.buffer(remaining);
            } catch (Exception e) {
                LOGGER.error("TDS handler error", e);
                socket.close();
            }
        }

        private void handlePreLogin(NetSocket socket) {
            // Minimal PreLogin response: VERSION (0x00) + ENCRYPTION (0x01) + TERMINATOR (0xFF).
            // Encryption is reported as NOT_SUP — clients must connect with encrypt=false.
            byte[] body = new byte[5 /* VERSION header */
                    + 5 /* ENCRYPTION header */
                    + 1 /* terminator */
                    + 6 /* version payload: u32 build + u16 sub */
                    + 1 /* encryption payload */];
            int pos = 0;
            int dataOffset = 5 + 5 + 1;

            // VERSION token
            body[pos++] = 0x00;
            body[pos++] = (byte) ((dataOffset >> 8) & 0xFF);
            body[pos++] = (byte) (dataOffset & 0xFF);
            body[pos++] = 0x00;
            body[pos++] = 0x06;
            int versionOffset = dataOffset;
            dataOffset += 6;

            // ENCRYPTION token
            body[pos++] = 0x01;
            body[pos++] = (byte) ((dataOffset >> 8) & 0xFF);
            body[pos++] = (byte) (dataOffset & 0xFF);
            body[pos++] = 0x00;
            body[pos++] = 0x01;
            int encryptionOffset = dataOffset;

            // TERMINATOR
            body[pos++] = (byte) 0xFF;

            // VERSION payload: 14.0.1000.169
            body[versionOffset] = 0x0E;
            body[versionOffset + 1] = 0x00;
            body[versionOffset + 2] = 0x03;
            body[versionOffset + 3] = (byte) 0xE8;
            body[versionOffset + 4] = 0x00;
            body[versionOffset + 5] = 0x00;

            // ENCRYPTION payload
            body[encryptionOffset] = ENCRYPT_NOT_SUP;

            socket.write(Buffer.buffer(TDSResponse.buildTDSPacket(TDS_TABULAR_RESULT, body)));
            state = State.LOGIN;
        }

        private void handleLogin7(NetSocket socket, byte[] bytes, int totalLength) {
            // Login7 body begins at absolute offset 8 (after TDS header).
            // OffsetLength table starts at body offset 36, each entry = u16 offset (relative to body start) + u16 length-in-chars.
            String username = "";
            String password = "";
            String dbName = "";
            int clientTdsVersion = TDS_VERSION_74; // assume 7.4 if parse fails
            try {
                int bodyStart = 8;
                // LOGIN7 body offset 4..7 = client TDSVersion (DWORD, BIG-ENDIAN on wire).
                if (bodyStart + 8 <= totalLength) {
                    clientTdsVersion = ((bytes[bodyStart + 4] & 0xFF) << 24)
                            | ((bytes[bodyStart + 5] & 0xFF) << 16)
                            | ((bytes[bodyStart + 6] & 0xFF) << 8)
                            | (bytes[bodyStart + 7] & 0xFF);
                }
                int olTable = bodyStart + 36;
                if (olTable + 64 <= totalLength) {
                    int userOff = readU16LE(bytes, olTable + 4);
                    int userLen = readU16LE(bytes, olTable + 6);
                    int pwdOff = readU16LE(bytes, olTable + 8);
                    int pwdLen = readU16LE(bytes, olTable + 10);
                    int dbOff = readU16LE(bytes, olTable + 32);
                    int dbLen = readU16LE(bytes, olTable + 34);

                    if (userLen > 0 && bodyStart + userOff + userLen * 2 <= totalLength) {
                        username = new String(bytes, bodyStart + userOff, userLen * 2, StandardCharsets.UTF_16LE);
                    }
                    if (pwdLen > 0 && bodyStart + pwdOff + pwdLen * 2 <= totalLength) {
                        byte[] obfuscated = new byte[pwdLen * 2];
                        System.arraycopy(bytes, bodyStart + pwdOff, obfuscated, 0, obfuscated.length);
                        password = unmanglePassword(obfuscated);
                    }
                    if (dbLen > 0 && bodyStart + dbOff + dbLen * 2 <= totalLength) {
                        dbName = new String(bytes, bodyStart + dbOff, dbLen * 2, StandardCharsets.UTF_16LE);
                    }
                }
            } catch (Exception e) {
                LOGGER.debug("TDS Login7 parse error: {}", e.getMessage());
            }
            String clientVersionName = tdsVersionName(clientTdsVersion);
            LOGGER.info("TDS Login7: user={} db={} client_tds={}", username, dbName, clientVersionName);

            // Server supports TDS 7.2 through 7.4. Older versions (7.0 = SQL Server 7.0,
            // 7.1 = SQL Server 2000) use a fundamentally different LOGIN7 layout and row
            // encoding — not supported here. Reject with a Pre-Login-style error.
            if (clientTdsVersion < TDS_VERSION_72) {
                LOGGER.warn("TDS: rejecting unsupported TDS version 0x{} (need >= 7.2 / SQL Server 2005)",
                        Integer.toHexString(clientTdsVersion));
                byte[] err = TDSResponse.buildErrorTokenBytes(18456,
                        "Mycat3 TDS front-end requires TDS 7.2 or higher (SQL Server 2005+). "
                                + "Got 0x" + Integer.toHexString(clientTdsVersion) + ". "
                                + "SQL Server 7.0 / 2000 clients are not supported.");
                byte[] done = TDSResponse.buildDoneTokenBytes(0, 0, 0x02);
                socket.write(Buffer.buffer(TDSResponse.buildTDSPacket(TDS_TABULAR_RESULT, concat(err, done))));
                socket.close();
                return;
            }

            // Negotiated TDS version = min(client, server max 7.4) per MS-TDS spec.
            int negotiatedTdsVersion = Math.min(clientTdsVersion, TDS_VERSION_74);

            if (backend == null) {
                LOGGER.error("TDS: no backend wired; closing");
                socket.close();
                return;
            }
            String host = remoteHost(socket);
            UserConfig userInfo = backend.authenticate(username, host, password);
            if (userInfo == null) {
                socket.write(Buffer.buffer(buildLoginError(username)));
                socket.close();
                return;
            }
            this.ctx = backend.createContext(username, host,
                    new InetSocketAddress(socket.remoteAddress().host(), socket.remoteAddress().port()),
                    userInfo, dbName);
            String resolvedSchema = ctx.getDefaultSchema();
            if (resolvedSchema != null && !resolvedSchema.isEmpty()) {
                this.database = resolvedSchema;
            } else if (!dbName.isEmpty()) {
                this.database = dbName;
            }

            // CRITICAL: one TDS packet must contain ALL response tokens (LOGINACK + ENVCHANGE +
            // INFO + DONE). Splitting them into 4 separate TDS packets — each with EOM=1 —
            // tells the client "you already got the whole message" after LOGINACK alone, with
            // no DONE; mssql-jdbc / jTDS then disconnect mid-handshake.
            // Real SQL Server emits a sequence of ENVCHANGE + INFO before LOGINACK +
            // a final ENVCHANGE for SQL_COLLATION. Without SQL_COLLATION specifically,
            // the sqljdbc4 driver's connection.collation stays null and any later RPC
            // call with Unicode parameters (sp_cursorprepexec, sp_executesql, ...)
            // NPEs inside TDSWriter.writeRPCStringUnicode. PACKET_SIZE and LANGUAGE
            // are sent for completeness — they're cheap and some drivers require them.
            byte[] tokens = concat(
                    buildEnvChangeDatabaseToken(this.database),
                    buildInfoTokenBytes("Changed database context to '" + this.database + "'."),
                    buildEnvChangeLanguageToken("us_english"),
                    buildInfoTokenBytes("Changed language setting to us_english."),
                    buildLoginAckToken(negotiatedTdsVersion),
                    buildEnvChangeCollationToken(),
                    buildEnvChangePacketSizeToken("4096"),
                    // DONE_FINAL (status=0), rowCount=0 (no rows affected by login).
                    TDSResponse.buildDoneTokenBytes(0, 0, 0));
            socket.write(Buffer.buffer(TDSResponse.buildTDSPacket(TDS_TABULAR_RESULT, tokens)));
            state = State.READY;
            LOGGER.info("TDS Login7 ack sent (tds={})", tdsVersionName(negotiatedTdsVersion));
        }

        private void handleSqlBatch(NetSocket socket, byte[] bytes, int totalLength, int packetType) {
            if (ctx == null) {
                LOGGER.warn("TDS SQL before login; closing");
                socket.close();
                return;
            }
            int pos = 8;
            if (packetType == TDS_RPC_REQUEST) {
                pos += 4;
            }
            int sqlBytes = totalLength - pos;
            if (sqlBytes <= 0) return;
            String sql = new String(bytes, pos, sqlBytes, StandardCharsets.UTF_16LE).trim();
            LOGGER.info("TDS SQL: {}", sql);

            backend.executeSql(sql, ctx, size -> new TDSResponse(socket, ctx, size))
                    .onFailure(t -> {
                        LOGGER.error("TDS SQL failed: {}", sql, t);
                        byte[] tokens = concat(
                                TDSResponse.buildErrorTokenBytes(50000, t.getMessage() == null ? "execution error" : t.getMessage()),
                                TDSResponse.buildDoneTokenBytes(0, 0, 0x02));
                        socket.write(Buffer.buffer(TDSResponse.buildTDSPacket(TDS_TABULAR_RESULT, tokens)));
                    });
        }
    }

    private static int readU16LE(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) | ((bytes[offset + 1] & 0xFF) << 8);
    }

    private static String unmanglePassword(byte[] obfuscated) {
        // Client encodes as: obf = swap_nibbles(plain) XOR 0xA5
        // To decode you must reverse the composition order: XOR first, then swap.
        // (Doing swap-then-XOR is *not* the inverse — swap doesn't distribute over XOR
        //  because swap(0xA5) == 0x5A != 0xA5.)
        byte[] plain = new byte[obfuscated.length];
        for (int i = 0; i < obfuscated.length; i++) {
            int xored = (obfuscated[i] & 0xFF) ^ 0xA5;
            plain[i] = (byte) (((xored << 4) | (xored >> 4)) & 0xFF);
        }
        return new String(plain, StandardCharsets.UTF_16LE);
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

    /** LOGINACK token (no TDS packet wrapper). Echoes the negotiated TDS version per MS-TDS spec. */
    private static byte[] buildLoginAckToken(int negotiatedTdsVersion) {
        byte[] progName = "Microsoft SQL Server".getBytes(StandardCharsets.UTF_16LE);
        int bodyLen = 1 /* interface */ + 4 /* TDS version */ + 1 /* progName length */
                + progName.length + 4 /* program version */;
        byte[] tokenBody = new byte[1 + 2 + bodyLen];
        int pos = 0;
        tokenBody[pos++] = TOKEN_LOGINACK;
        tokenBody[pos++] = (byte) (bodyLen & 0xFF);
        tokenBody[pos++] = (byte) ((bodyLen >> 8) & 0xFF);
        tokenBody[pos++] = 0x01; // Interface = SQL_TSQL
        // TDSVersion is a DWORD written BIG-ENDIAN on the wire.
        tokenBody[pos++] = (byte) ((negotiatedTdsVersion >> 24) & 0xFF);
        tokenBody[pos++] = (byte) ((negotiatedTdsVersion >> 16) & 0xFF);
        tokenBody[pos++] = (byte) ((negotiatedTdsVersion >> 8) & 0xFF);
        tokenBody[pos++] = (byte) (negotiatedTdsVersion & 0xFF);
        tokenBody[pos++] = (byte) (progName.length / 2);
        System.arraycopy(progName, 0, tokenBody, pos, progName.length); pos += progName.length;
        // ProgVersion: bytes are Major.Minor.BuildHi.BuildLo (big-endian "build").
        // Match the negotiated TDS to a SQL Server product version so old clients
        // don't see "TDS 7.3B but server claims SQL Server 2017" and bail on the
        // sanity check.
        int[] progVer = progVersionFor(negotiatedTdsVersion);
        tokenBody[pos++] = (byte) progVer[0];
        tokenBody[pos++] = (byte) progVer[1];
        tokenBody[pos++] = (byte) ((progVer[2] >> 8) & 0xFF);
        tokenBody[pos]   = (byte) (progVer[2] & 0xFF);
        return tokenBody;
    }

    /** Returns {major, minor, build} matching the SQL Server product version for a given TDS protocol. */
    private static int[] progVersionFor(int tdsVersion) {
        switch (tdsVersion) {
            case TDS_VERSION_72:  return new int[]{ 9, 0, 5000};   // SQL Server 2005 SP4
            case TDS_VERSION_73A: return new int[]{10, 0, 6000};   // SQL Server 2008 SP4
            case TDS_VERSION_73B: return new int[]{10, 50, 6000};  // SQL Server 2008 R2 SP3
            case TDS_VERSION_74:
            default:              return new int[]{14, 0, 1000};   // SQL Server 2017
        }
    }

    private static String tdsVersionName(int v) {
        switch (v) {
            case TDS_VERSION_70:  return "7.0 (SQL Server 7.0)";
            case TDS_VERSION_71:  return "7.1 (SQL Server 2000)";
            case TDS_VERSION_72:  return "7.2 (SQL Server 2005)";
            case TDS_VERSION_73A: return "7.3A (SQL Server 2008)";
            case TDS_VERSION_73B: return "7.3B (SQL Server 2008 R2)";
            case TDS_VERSION_74:  return "7.4 (SQL Server 2012+)";
            default: return "0x" + Integer.toHexString(v);
        }
    }

    /** ENVCHANGE token, type 1 = DATABASE. NEW + OLD are both B_VARCHAR (char-count + UTF-16LE). */
    private static byte[] buildEnvChangeDatabaseToken(String database) {
        return buildEnvChangeBVarcharPair((byte) 0x01, database, "master");
    }

    /** ENVCHANGE token, type 2 = LANGUAGE. NEW + OLD are both B_VARCHAR. */
    private static byte[] buildEnvChangeLanguageToken(String language) {
        return buildEnvChangeBVarcharPair((byte) 0x02, language, "");
    }

    /** ENVCHANGE token, type 4 = PACKET_SIZE. NEW + OLD are both B_VARCHAR (e.g. "4096"). */
    private static byte[] buildEnvChangePacketSizeToken(String newPacketSize) {
        return buildEnvChangeBVarcharPair((byte) 0x04, newPacketSize, newPacketSize);
    }

    /**
     * ENVCHANGE token, type 7 = SQL_COLLATION. NEW + OLD are 5-byte SQL Collation each,
     * preceded by a 1-byte length. Latin1_General_CI_AS_KS_WS (0x0409 + flags 0xD0 + sortid 0x34).
     * This is the token sqljdbc4 needs to populate its connection.collation field — without it
     * any later writeRPCStringUnicode call NPEs.
     */
    private static byte[] buildEnvChangeCollationToken() {
        byte[] collation = {0x09, 0x04, (byte) 0xD0, 0x00, 0x34};
        int bodyLen = 1 /* type */ + 1 /* newLen */ + collation.length + 1 /* oldLen */ + collation.length;
        byte[] tokenBody = new byte[1 + 2 + bodyLen];
        int pos = 0;
        tokenBody[pos++] = TOKEN_ENVCHANGE;
        tokenBody[pos++] = (byte) (bodyLen & 0xFF);
        tokenBody[pos++] = (byte) ((bodyLen >> 8) & 0xFF);
        tokenBody[pos++] = 0x07; // SQL_COLLATION
        tokenBody[pos++] = (byte) collation.length;
        System.arraycopy(collation, 0, tokenBody, pos, collation.length); pos += collation.length;
        tokenBody[pos++] = (byte) collation.length;
        System.arraycopy(collation, 0, tokenBody, pos, collation.length);
        return tokenBody;
    }

    /** Helper for ENVCHANGE types where both NEW and OLD values are B_VARCHAR. */
    private static byte[] buildEnvChangeBVarcharPair(byte envType, String newValue, String oldValue) {
        byte[] newBytes = newValue.getBytes(StandardCharsets.UTF_16LE);
        byte[] oldBytes = oldValue.getBytes(StandardCharsets.UTF_16LE);
        int bodyLen = 1 /* envType */ + 1 /* newLen */ + newBytes.length + 1 /* oldLen */ + oldBytes.length;
        byte[] tokenBody = new byte[1 + 2 + bodyLen];
        int pos = 0;
        tokenBody[pos++] = TOKEN_ENVCHANGE;
        tokenBody[pos++] = (byte) (bodyLen & 0xFF);
        tokenBody[pos++] = (byte) ((bodyLen >> 8) & 0xFF);
        tokenBody[pos++] = envType;
        tokenBody[pos++] = (byte) (newBytes.length / 2);
        System.arraycopy(newBytes, 0, tokenBody, pos, newBytes.length); pos += newBytes.length;
        tokenBody[pos++] = (byte) (oldBytes.length / 2);
        System.arraycopy(oldBytes, 0, tokenBody, pos, oldBytes.length);
        return tokenBody;
    }

    /** INFO token (no TDS packet wrapper). */
    private static byte[] buildInfoTokenBytes(String message) {
        byte[] msg = message.getBytes(StandardCharsets.UTF_16LE);
        byte[] srv = "Mycat3-TDS".getBytes(StandardCharsets.UTF_16LE);
        int bodyLen = 4 /* number */ + 1 /* state */ + 1 /* class */
                + 2 /* MsgText USHORT */ + msg.length
                + 1 + srv.length
                + 1 /* proc len */
                + 4 /* LineNumber */;
        byte[] tokenBody = new byte[1 + 2 + bodyLen];
        int pos = 0;
        tokenBody[pos++] = TOKEN_INFO;
        tokenBody[pos++] = (byte) (bodyLen & 0xFF);
        tokenBody[pos++] = (byte) ((bodyLen >> 8) & 0xFF);
        // Number = 5701 (INT32 LE)
        tokenBody[pos++] = 0x45; tokenBody[pos++] = 0x16; tokenBody[pos++] = 0x00; tokenBody[pos++] = 0x00;
        tokenBody[pos++] = 0x01; // State
        tokenBody[pos++] = 0x00; // Class
        // MsgText length is US_VARCHAR (USHORT char-count, LE).
        int msgChars = msg.length / 2;
        tokenBody[pos++] = (byte) (msgChars & 0xFF);
        tokenBody[pos++] = (byte) ((msgChars >> 8) & 0xFF);
        System.arraycopy(msg, 0, tokenBody, pos, msg.length); pos += msg.length;
        tokenBody[pos++] = (byte) (srv.length / 2);
        System.arraycopy(srv, 0, tokenBody, pos, srv.length); pos += srv.length;
        tokenBody[pos++] = 0x00; // proc length (B_VARCHAR)
        // LineNumber (INT32 LE) = 0
        tokenBody[pos++] = 0x00; tokenBody[pos++] = 0x00; tokenBody[pos++] = 0x00; tokenBody[pos] = 0x00;
        return tokenBody;
    }

    private static byte[] buildLoginError(String username) {
        byte[] err = TDSResponse.buildErrorTokenBytes(18456, "Login failed for user '" + username + "'.");
        byte[] done = TDSResponse.buildDoneTokenBytes(0, 0, 0x02);
        return TDSResponse.buildTDSPacket(TDS_TABULAR_RESULT, concat(err, done));
    }
}
