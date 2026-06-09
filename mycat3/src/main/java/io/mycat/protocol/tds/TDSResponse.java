package io.mycat.protocol.tds;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.protocol.api.AbstractMycatProtocolResponse;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * TDS wire-format implementation of {@link AbstractMycatProtocolResponse}.
 *
 * Wire model: TDS messages contain a sequence of TOKENS inside one (or more)
 * TDS packets. The terminal packet has Status=0x01 (EOM). Token builders in
 * this class therefore return RAW token bytes; {@link #buildTDSPacket} wraps
 * the concatenation of all tokens at message boundary. Wrapping every token
 * in its own TDS packet (each with EOM=1) breaks the client — it interprets
 * each as a complete reply and disconnects when DONE is missing.
 */
public class TDSResponse extends AbstractMycatProtocolResponse {

    static final byte TDS_TABULAR_RESULT = 0x04;
    private static final byte TOKEN_DONE = (byte) 0xFD;
    private static final byte TOKEN_COLMETADATA = (byte) 0x81;
    private static final byte TOKEN_ROW = (byte) 0xD1;
    private static final byte TOKEN_ERROR = (byte) 0xAA;

    private final NetSocket socket;

    public TDSResponse(NetSocket socket, MycatDataContext ctx, int stmtSize) {
        super(ctx, stmtSize);
        this.socket = socket;
    }

    @Override
    protected Future<Void> writeResultSet(MycatRowMetaData metadata, List<Object[]> rows, boolean hasMore) {
        int columnCount = metadata.getColumnCount();
        String[] columns = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns[i] = metadata.getColumnName(i);
        }
        Buffer tokens = Buffer.buffer();
        tokens.appendBytes(buildColMetaDataBytes(columns));
        for (Object[] row : rows) {
            String[] strRow = new String[row.length];
            for (int j = 0; j < row.length; j++) {
                strRow[j] = row[j] == null ? "" : String.valueOf(row[j]);
            }
            tokens.appendBytes(buildRowDataBytes(strRow));
        }
        tokens.appendBytes(buildDoneTokenBytes(rows.size(), 0, hasMore ? 0x01 : 0x00));
        socket.write(Buffer.buffer(buildTDSPacket(TDS_TABULAR_RESULT, tokens.getBytes())));
        return Future.succeededFuture();
    }

    @Override
    protected Future<Void> writeOk(long affectedRow, long lastInsertId, boolean hasMore) {
        byte[] done = buildDoneTokenBytes(affectedRow, 0, hasMore ? 0x01 : 0x00);
        socket.write(Buffer.buffer(buildTDSPacket(TDS_TABULAR_RESULT, done)));
        return Future.succeededFuture();
    }

    @Override
    protected Future<Void> writeError(String message, int code) {
        Buffer tokens = Buffer.buffer();
        tokens.appendBytes(buildErrorTokenBytes(code == 0 ? 50000 : code, message == null ? "execution error" : message));
        tokens.appendBytes(buildDoneTokenBytes(0, 0, 0x02));
        socket.write(Buffer.buffer(buildTDSPacket(TDS_TABULAR_RESULT, tokens.getBytes())));
        return Future.succeededFuture();
    }

    /**
     * COLMETADATA token (raw, no TDS packet wrapper). Per MS-TDS §2.2.7.4.
     *
     * One column entry layout (NVARCHAR(4000) — matches our UTF-16LE row encoding):
     *   UserType   DWORD            00 00 00 00
     *   Flags      USHORT LE        09 00       (Nullable=1, Updateable bits)
     *   TYPE_ID    BYTE             E7          (NVARCHARTYPE — Unicode varchar)
     *   MaxLength  USHORT LE        40 1F       (8000 bytes = 4000 chars)
     *   Collation  5 bytes          09 04 D0 00 34  (Latin1_General_CI_AS_KS_WS)
     *   NameLen    BYTE             N           (chars, not bytes)
     *   Name       UTF-16LE 2N bytes
     *
     * Per-column overhead = 15 + 2N bytes.
     */
    static byte[] buildColMetaDataBytes(String[] columns) {
        int totalLen = 1 /* TYPE */ + 2 /* COUNT */;
        for (String col : columns) {
            byte[] nameBytes = col.getBytes(StandardCharsets.UTF_16LE);
            totalLen += 4 + 2 + 1 + 2 + 5 + 1 + nameBytes.length;
        }
        byte[] body = new byte[totalLen];
        int pos = 0;
        body[pos++] = TOKEN_COLMETADATA;
        body[pos++] = (byte) (columns.length & 0xFF);
        body[pos++] = (byte) ((columns.length >> 8) & 0xFF);
        for (String col : columns) {
            byte[] nameBytes = col.getBytes(StandardCharsets.UTF_16LE);
            // UserType = 0
            body[pos++] = 0; body[pos++] = 0; body[pos++] = 0; body[pos++] = 0;
            // Flags USHORT LE = 0x0009 (Nullable + Updateable)
            body[pos++] = 0x09; body[pos++] = 0x00;
            // TYPE_ID = NVARCHARTYPE (Unicode varchar)
            body[pos++] = (byte) 0xE7;
            // MaxLength USHORT LE = 8000 bytes = 4000 NVARCHAR chars (max non-MAX)
            body[pos++] = (byte) 0x40; body[pos++] = (byte) 0x1F;
            // Collation: LCID 0x0409 (en-US) + flags 0xD0 + version 0 + sortid 0x34 (Latin1_General_CI_AS_KS_WS)
            body[pos++] = 0x09; body[pos++] = 0x04; body[pos++] = (byte) 0xD0; body[pos++] = 0x00; body[pos++] = 0x34;
            // ColName: B_VARCHAR
            body[pos++] = (byte) (nameBytes.length / 2);
            System.arraycopy(nameBytes, 0, body, pos, nameBytes.length);
            pos += nameBytes.length;
        }
        return body;
    }

    /** ROW token (raw, no TDS packet wrapper). */
    static byte[] buildRowDataBytes(String[] values) {
        int bodyLen = 1;
        byte[][] encoded = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            byte[] vb = values[i].getBytes(StandardCharsets.UTF_16LE);
            encoded[i] = new byte[2 + vb.length];
            encoded[i][0] = (byte) (vb.length & 0xFF);
            encoded[i][1] = (byte) ((vb.length >> 8) & 0xFF);
            System.arraycopy(vb, 0, encoded[i], 2, vb.length);
            bodyLen += encoded[i].length;
        }
        byte[] body = new byte[bodyLen];
        int pos = 0;
        body[pos++] = TOKEN_ROW;
        for (byte[] enc : encoded) {
            System.arraycopy(enc, 0, body, pos, enc.length);
            pos += enc.length;
        }
        return body;
    }

    /** DONE token (raw, no TDS packet wrapper). 13 bytes: type + Status (USHORT LE) + CurCmd (USHORT LE) + DoneRowCount (ULONGLONG LE). */
    static byte[] buildDoneTokenBytes(long rowCount, int curCmd, int doneStatus) {
        byte[] body = new byte[13];
        body[0] = TOKEN_DONE;
        body[1] = (byte) (doneStatus & 0xFF);
        body[2] = (byte) ((doneStatus >> 8) & 0xFF);
        body[3] = (byte) (curCmd & 0xFF);
        body[4] = (byte) ((curCmd >> 8) & 0xFF);
        body[5] = (byte) (rowCount & 0xFF);
        body[6] = (byte) ((rowCount >> 8) & 0xFF);
        body[7] = (byte) ((rowCount >> 16) & 0xFF);
        body[8] = (byte) ((rowCount >> 24) & 0xFF);
        body[9] = 0x00; body[10] = 0x00; body[11] = 0x00; body[12] = 0x00;
        return body;
    }

    /** ERROR token (raw, no TDS packet wrapper). */
    static byte[] buildErrorTokenBytes(int errorCode, String message) {
        byte[] msg = message.getBytes(StandardCharsets.UTF_16LE);
        byte[] srv = "Mycat3-TDS".getBytes(StandardCharsets.UTF_16LE);
        int msgChars = msg.length / 2;
        // Per MS-TDS §2.2.7.10:
        //   Length (USHORT LE), Number (INT32 LE), State (BYTE), Severity (BYTE),
        //   MsgText (US_VARCHAR = USHORT char-count + UTF-16LE),
        //   ServerName (B_VARCHAR = BYTE char-count + UTF-16LE),
        //   ProcName (B_VARCHAR), LineNumber (INT32 LE).
        int bodyLen = 4 /* Number */ + 1 /* State */ + 1 /* Severity */
                + 2 /* MsgText USHORT */ + msg.length
                + 1 /* ServerName BYTE */ + srv.length
                + 1 /* ProcName BYTE */
                + 4 /* LineNumber */;
        byte[] body = new byte[1 /* type */ + 2 /* length USHORT */ + bodyLen];
        int pos = 0;
        body[pos++] = TOKEN_ERROR;
        body[pos++] = (byte) (bodyLen & 0xFF);
        body[pos++] = (byte) ((bodyLen >> 8) & 0xFF);
        System.arraycopy(intToBytesLE(errorCode), 0, body, pos, 4); pos += 4;
        body[pos++] = 0x01; // State
        body[pos++] = 0x10; // Severity = 16
        body[pos++] = (byte) (msgChars & 0xFF);
        body[pos++] = (byte) ((msgChars >> 8) & 0xFF);
        System.arraycopy(msg, 0, body, pos, msg.length); pos += msg.length;
        body[pos++] = (byte) (srv.length / 2);
        System.arraycopy(srv, 0, body, pos, srv.length); pos += srv.length;
        body[pos++] = 0x00; // ProcName length
        body[pos++] = 0x00; body[pos++] = 0x00; body[pos++] = 0x00; body[pos] = 0x00; // LineNumber = 0
        return body;
    }

    /** Wrap a sequence of token bytes in one TDS packet with Status=EOM. */
    static byte[] buildTDSPacket(byte packetType, byte[] payload) {
        int totalLen = 8 + payload.length;
        byte[] packet = new byte[totalLen];
        packet[0] = packetType;
        packet[1] = 0x01; // Status: EOM (end of message)
        packet[2] = (byte) ((totalLen >> 8) & 0xFF); // Length is BIG-ENDIAN
        packet[3] = (byte) (totalLen & 0xFF);
        packet[4] = 0x00; packet[5] = 0x00; // SPID
        packet[6] = 0x00; // PacketID
        packet[7] = 0x00; // Window
        System.arraycopy(payload, 0, packet, 8, payload.length);
        return packet;
    }

    static byte[] intToBytesLE(int value) {
        return new byte[]{
                (byte) value,
                (byte) (value >>> 8),
                (byte) (value >>> 16),
                (byte) (value >>> 24)
        };
    }
}
