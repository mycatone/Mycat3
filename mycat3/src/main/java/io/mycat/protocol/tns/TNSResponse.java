package io.mycat.protocol.tns;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.protocol.api.AbstractMycatProtocolResponse;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Minimal TNS wire-format implementation of {@link AbstractMycatProtocolResponse}.
 * The byte layout here is intentionally simplified — see Mycat3配置说明.md §1.1.
 */
public class TNSResponse extends AbstractMycatProtocolResponse {

    private static final int TNS_DATA = 6;

    private final NetSocket socket;

    public TNSResponse(NetSocket socket, MycatDataContext ctx, int stmtSize) {
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
        Buffer buf = Buffer.buffer();
        buf.appendBytes(buildColumnDesc(columns));
        for (Object[] row : rows) {
            String[] strRow = new String[row.length];
            for (int j = 0; j < row.length; j++) {
                strRow[j] = row[j] == null ? "" : String.valueOf(row[j]);
            }
            buf.appendBytes(buildRow(strRow));
        }
        buf.appendBytes(buildQueryStatus(rows.size()));
        socket.write(buf);
        return Future.succeededFuture();
    }

    @Override
    protected Future<Void> writeOk(long affectedRow, long lastInsertId, boolean hasMore) {
        socket.write(Buffer.buffer(buildDmlStatus(affectedRow)));
        return Future.succeededFuture();
    }

    @Override
    protected Future<Void> writeError(String message, int code) {
        socket.write(Buffer.buffer(buildErrorStatus(message == null ? "execution error" : message, code)));
        return Future.succeededFuture();
    }

    private static byte[] buildColumnDesc(String[] columns) {
        int bodyLen = 5;
        for (String col : columns) {
            bodyLen += 16 + col.getBytes(StandardCharsets.UTF_8).length + 1;
        }
        byte[] tnsHeader = buildTNSHeader(TNS_DATA, bodyLen);
        byte[] fullBuf = new byte[8 + bodyLen];
        System.arraycopy(tnsHeader, 0, fullBuf, 0, 8);
        int pos = 8;
        fullBuf[pos++] = 0x04;
        fullBuf[pos++] = 0x01;
        fullBuf[pos++] = (byte) TNS_DATA;
        fullBuf[pos++] = (byte) (columns.length >> 8);
        fullBuf[pos++] = (byte) (columns.length & 0xFF);
        for (String col : columns) {
            byte[] nameBytes = col.getBytes(StandardCharsets.UTF_8);
            fullBuf[pos++] = (byte) nameBytes.length;
            System.arraycopy(nameBytes, 0, fullBuf, pos, nameBytes.length);
            pos += nameBytes.length;
            pos += 15;
        }
        return fullBuf;
    }

    private static byte[] buildRow(String[] values) {
        int bodyLen = 2;
        byte[][] encoded = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            byte[] vb = values[i].getBytes(StandardCharsets.UTF_8);
            encoded[i] = new byte[3 + vb.length];
            encoded[i][0] = 0x01;
            encoded[i][1] = (byte) (vb.length >> 8);
            encoded[i][2] = (byte) (vb.length & 0xFF);
            System.arraycopy(vb, 0, encoded[i], 3, vb.length);
            bodyLen += encoded[i].length;
        }
        byte[] tnsHeader = buildTNSHeader(TNS_DATA, bodyLen);
        byte[] fullBuf = new byte[8 + bodyLen];
        System.arraycopy(tnsHeader, 0, fullBuf, 0, 8);
        int pos = 8;
        fullBuf[pos++] = (byte) (values.length >> 8);
        fullBuf[pos++] = (byte) (values.length & 0xFF);
        for (byte[] enc : encoded) {
            System.arraycopy(enc, 0, fullBuf, pos, enc.length);
            pos += enc.length;
        }
        return fullBuf;
    }

    private static byte[] buildQueryStatus(long rowCount) {
        int bodyLen = 6;
        byte[] tnsHeader = buildTNSHeader(TNS_DATA, bodyLen);
        byte[] buf = new byte[8 + bodyLen];
        System.arraycopy(tnsHeader, 0, buf, 0, 8);
        buf[8] = 0x08; buf[9] = 0x01; buf[10] = (byte) TNS_DATA;
        buf[11] = (byte) (rowCount >> 8);
        buf[12] = (byte) (rowCount & 0xFF);
        buf[13] = 0x00;
        return buf;
    }

    private static byte[] buildDmlStatus(long affected) {
        int bodyLen = 6;
        byte[] tnsHeader = buildTNSHeader(TNS_DATA, bodyLen);
        byte[] buf = new byte[8 + bodyLen];
        System.arraycopy(tnsHeader, 0, buf, 0, 8);
        buf[8] = 0x08; buf[9] = 0x01; buf[10] = (byte) TNS_DATA;
        buf[11] = (byte) (affected >> 8);
        buf[12] = (byte) (affected & 0xFF);
        buf[13] = 0x00;
        return buf;
    }

    private static byte[] buildErrorStatus(String message, int errorCode) {
        byte[] msg = message.getBytes(StandardCharsets.UTF_8);
        int bodyLen = 8 + msg.length;
        byte[] tnsHeader = buildTNSHeader(TNS_DATA, bodyLen);
        byte[] buf = new byte[8 + bodyLen];
        System.arraycopy(tnsHeader, 0, buf, 0, 8);
        int pos = 8;
        buf[pos++] = 0x04;
        buf[pos++] = 0x01;
        buf[pos++] = (byte) TNS_DATA;
        buf[pos++] = (byte) ((errorCode >> 8) & 0xFF);
        buf[pos++] = (byte) (errorCode & 0xFF);
        buf[pos++] = 0x00;
        buf[pos++] = (byte) (msg.length & 0xFF);
        buf[pos++] = 0x00;
        System.arraycopy(msg, 0, buf, pos, msg.length);
        return buf;
    }

    private static byte[] buildTNSHeader(int packetType, int payloadLength) {
        byte[] buf = new byte[8];
        int totalLen = 8 + payloadLength;
        buf[0] = (byte) (totalLen >> 8);
        buf[1] = (byte) (totalLen & 0xFF);
        buf[2] = 0x00; buf[3] = 0x00;
        buf[4] = (byte) packetType;
        buf[5] = 0x00; buf[6] = 0x00; buf[7] = 0x00;
        return buf;
    }
}
