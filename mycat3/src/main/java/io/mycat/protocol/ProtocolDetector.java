package io.mycat.protocol;

import io.vertx.core.buffer.Buffer;

public class ProtocolDetector {

    private ProtocolDetector() {
    }

    public static String detect(Buffer buf) {
        if (buf == null || buf.length() < 4) {
            return null;
        }
        byte[] bytes = buf.getBytes();

        if (detectMySQL(bytes)) {
            return "mysql";
        }
        if (detectTDS(bytes)) {
            return "sqlserver";
        }
        if (detectTNS(bytes)) {
            return "oracle";
        }
        if (detectPG(bytes)) {
            return "postgresql";
        }
        return null;
    }

    private static boolean detectMySQL(byte[] bytes) {
        if (bytes.length < 5) return false;
        int packetLength = (bytes[0] & 0xFF) | ((bytes[1] & 0xFF) << 8) | ((bytes[2] & 0xFF) << 16);
        int sequenceId = bytes[3] & 0xFF;
        if (sequenceId == 1 && packetLength > 32 && bytes.length >= 8) {
            int capLow = (bytes[4] & 0xFF) | ((bytes[5] & 0xFF) << 8);
            return capLow > 0;
        }
        return false;
    }

    private static boolean detectTDS(byte[] bytes) {
        if (bytes.length < 1) return false;
        int firstByte = bytes[0] & 0xFF;
        return firstByte == 0x12 || firstByte == 0x10
                || firstByte == 0x01 || firstByte == 0x03;
    }

    private static boolean detectTNS(byte[] bytes) {
        if (bytes.length < 10) return false;
        int packetLength = ((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF);
        if (packetLength > 0 && packetLength < 0xFFFF) {
            int packetType = bytes[4] & 0xFF;
            return packetType == 1;
        }
        return false;
    }

    private static boolean detectPG(byte[] bytes) {
        if (bytes.length < 8) return false;
        int length = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16)
                | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
        if (length > 0 && length < 0xFFFFFF) {
            int protocolVersion = ((bytes[4] & 0xFF) << 24) | ((bytes[5] & 0xFF) << 16)
                    | ((bytes[6] & 0xFF) << 8) | (bytes[7] & 0xFF);
            return protocolVersion == 196608 || protocolVersion == 80877103;
        }
        return false;
    }
}