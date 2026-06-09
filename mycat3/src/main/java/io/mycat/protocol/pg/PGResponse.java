package io.mycat.protocol.pg;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.protocol.api.AbstractMycatProtocolResponse;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * PostgreSQL wire-format implementation of {@link AbstractMycatProtocolResponse}.
 * The base class drains Mycat's SQL pipeline; this class only knows how to spell
 * RowDescription / DataRow / CommandComplete / ErrorResponse / ReadyForQuery.
 *
 * Multi-statement note: a single PG simple-Query ({@code 'Q'}) may produce
 * multiple T-D-C blocks, but only one trailing 'Z' ReadyForQuery. So we emit
 * {@code Z} only when {@code hasMore == false}.
 */
public class PGResponse extends AbstractMycatProtocolResponse {

    private final NetSocket socket;
    private final String latestSqlHint;

    public PGResponse(NetSocket socket, MycatDataContext ctx, int stmtSize, String latestSqlHint) {
        super(ctx, stmtSize);
        this.socket = socket;
        this.latestSqlHint = latestSqlHint == null ? "" : latestSqlHint;
    }

    @Override
    protected Future<Void> writeResultSet(MycatRowMetaData metadata, List<Object[]> rows, boolean hasMore) {
        Buffer buf = Buffer.buffer();
        int columnCount = metadata.getColumnCount();
        String[] columns = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns[i] = metadata.getColumnName(i);
        }
        buf.appendBytes(buildRowDescription(columns));
        for (Object[] row : rows) {
            String[] strRow = new String[row.length];
            for (int j = 0; j < row.length; j++) {
                strRow[j] = row[j] == null ? null : String.valueOf(row[j]);
            }
            buf.appendBytes(buildDataRow(strRow));
        }
        buf.appendBytes(buildCommandComplete("SELECT " + rows.size()));
        if (!hasMore) {
            buf.appendBytes(buildReadyForQuery((byte) 'I'));
        }
        socket.write(buf);
        return Future.succeededFuture();
    }

    @Override
    protected Future<Void> writeOk(long affectedRow, long lastInsertId, boolean hasMore) {
        Buffer buf = Buffer.buffer();
        buf.appendBytes(buildCommandComplete(commandTagFor(latestSqlHint, affectedRow)));
        if (!hasMore) {
            buf.appendBytes(buildReadyForQuery((byte) 'I'));
        }
        socket.write(buf);
        return Future.succeededFuture();
    }

    @Override
    protected Future<Void> writeError(String message, int code) {
        Buffer buf = Buffer.buffer();
        buf.appendBytes(buildErrorResponse(message == null ? "execution error" : message));
        buf.appendBytes(buildReadyForQuery((byte) 'I'));
        socket.write(buf);
        return Future.succeededFuture();
    }

    private static String commandTagFor(String sql, long affected) {
        if (sql == null) return "OK " + affected;
        String head = sql.length() > 6 ? sql.substring(0, 6) : sql;
        String upper = head.toUpperCase(Locale.ROOT);
        if (upper.startsWith("INSERT")) return "INSERT 0 " + affected;
        if (upper.startsWith("UPDATE")) return "UPDATE " + affected;
        if (upper.startsWith("DELETE")) return "DELETE " + affected;
        if (upper.startsWith("BEGIN") || upper.startsWith("START ")) return "BEGIN";
        if (upper.startsWith("COMMIT")) return "COMMIT";
        if (upper.startsWith("ROLLBA")) return "ROLLBACK";
        return "OK " + affected;
    }

    static byte[] buildRowDescription(String[] columns) {
        int bodyLen = 2;
        for (String col : columns) {
            bodyLen += col.getBytes(StandardCharsets.UTF_8).length + 1 + 18;
        }
        byte[] buf = new byte[1 + 4 + bodyLen];
        buf[0] = 'T';
        System.arraycopy(intToBytes(4 + bodyLen), 0, buf, 1, 4);
        int pos = 5;
        System.arraycopy(shortToBytes(columns.length), 0, buf, pos, 2);
        pos += 2;
        for (String column : columns) {
            byte[] nameBytes = column.getBytes(StandardCharsets.UTF_8);
            System.arraycopy(nameBytes, 0, buf, pos, nameBytes.length);
            pos += nameBytes.length;
            buf[pos++] = 0x00;
            System.arraycopy(new byte[4], 0, buf, pos, 4); pos += 4;
            System.arraycopy(new byte[2], 0, buf, pos, 2); pos += 2;
            System.arraycopy(intToBytes(25), 0, buf, pos, 4); pos += 4;
            buf[pos++] = (byte) 0xFF; buf[pos++] = (byte) 0xFF;
            System.arraycopy(intToBytes(-1), 0, buf, pos, 4); pos += 4;
            System.arraycopy(new byte[2], 0, buf, pos, 2); pos += 2;
        }
        return buf;
    }

    static byte[] buildDataRow(String[] values) {
        int bodyLen = 2;
        byte[][] encoded = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                encoded[i] = intToBytes(-1);
            } else {
                byte[] valBytes = values[i].getBytes(StandardCharsets.UTF_8);
                encoded[i] = new byte[4 + valBytes.length];
                System.arraycopy(intToBytes(valBytes.length), 0, encoded[i], 0, 4);
                System.arraycopy(valBytes, 0, encoded[i], 4, valBytes.length);
            }
            bodyLen += encoded[i].length;
        }
        byte[] buf = new byte[1 + 4 + bodyLen];
        buf[0] = 'D';
        System.arraycopy(intToBytes(4 + bodyLen), 0, buf, 1, 4);
        int pos = 5;
        System.arraycopy(shortToBytes(values.length), 0, buf, pos, 2);
        pos += 2;
        for (byte[] enc : encoded) {
            System.arraycopy(enc, 0, buf, pos, enc.length);
            pos += enc.length;
        }
        return buf;
    }

    static byte[] buildCommandComplete(String tag) {
        byte[] tagBytes = tag.getBytes(StandardCharsets.UTF_8);
        int len = 4 + tagBytes.length + 1;
        byte[] buf = new byte[1 + len];
        buf[0] = 'C';
        System.arraycopy(intToBytes(len), 0, buf, 1, 4);
        System.arraycopy(tagBytes, 0, buf, 5, tagBytes.length);
        buf[5 + tagBytes.length] = 0x00;
        return buf;
    }

    static byte[] buildReadyForQuery(byte status) {
        byte[] buf = new byte[6];
        buf[0] = 'Z';
        System.arraycopy(intToBytes(5), 0, buf, 1, 4);
        buf[5] = status;
        return buf;
    }

    static byte[] buildErrorResponse(String errorMsg) {
        byte[] msg = errorMsg.getBytes(StandardCharsets.UTF_8);
        byte[] sev = "ERROR".getBytes(StandardCharsets.UTF_8);
        int bodyLen = 1 + sev.length + 1 + 1 + msg.length + 1 + 1;
        byte[] buf = new byte[1 + 4 + bodyLen];
        buf[0] = 'E';
        System.arraycopy(intToBytes(4 + bodyLen), 0, buf, 1, 4);
        int pos = 5;
        buf[pos++] = 'S';
        System.arraycopy(sev, 0, buf, pos, sev.length); pos += sev.length;
        buf[pos++] = 0x00;
        buf[pos++] = 'M';
        System.arraycopy(msg, 0, buf, pos, msg.length); pos += msg.length;
        buf[pos++] = 0x00;
        buf[pos] = 0x00;
        return buf;
    }

    static byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value
        };
    }

    static byte[] shortToBytes(int value) {
        return new byte[]{(byte) (value >> 8), (byte) (value & 0xFF)};
    }
}
