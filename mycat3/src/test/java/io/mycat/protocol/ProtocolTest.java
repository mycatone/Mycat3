package io.mycat.protocol;

import io.mycat.engine.InMemoryDB;
import io.mycat.frontend.QueryResponse;
import io.mycat.frontend.SqlRequest;
import io.mycat.protocol.api.NetworkChannel;
import io.mycat.protocol.api.ProtocolInfo;
import io.mycat.protocol.api.VertxNetworkChannel;
import io.mycat.protocol.mysql.MySQLProtocolHandler;
import io.mycat.protocol.pg.PGProtocolHandler;
import io.mycat.protocol.tds.TDSProtocolHandler;
import io.mycat.protocol.tns.TNSProtocolHandler;
import io.vertx.core.buffer.Buffer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProtocolTest {

    @Test
    public void testProtocolInfo() {
        ProtocolInfo info = new ProtocolInfo("mysql", 3306);
        Assert.assertEquals("mysql", info.getName());
        Assert.assertEquals(3306, info.getDefaultPort());
    }

    @Test
    public void testProtocolHandlerInterfaces() {
        MySQLProtocolHandler mysql = new MySQLProtocolHandler();
        Assert.assertEquals("mysql", mysql.getProtocolName());
        Assert.assertEquals(3306, mysql.getDefaultPort());

        TDSProtocolHandler tds = new TDSProtocolHandler();
        Assert.assertEquals("sqlserver", tds.getProtocolName());
        Assert.assertEquals(1433, tds.getDefaultPort());

        TNSProtocolHandler tns = new TNSProtocolHandler();
        Assert.assertEquals("oracle", tns.getProtocolName());
        Assert.assertEquals(1521, tns.getDefaultPort());

        PGProtocolHandler pg = new PGProtocolHandler();
        Assert.assertEquals("postgresql", pg.getProtocolName());
        Assert.assertEquals(5432, pg.getDefaultPort());
    }

    @Test
    public void testProtocolRegistry() {
        ProtocolRegistry.register(new MySQLProtocolHandler());
        ProtocolRegistry.register(new TDSProtocolHandler());
        ProtocolRegistry.register(new TNSProtocolHandler());
        ProtocolRegistry.register(new PGProtocolHandler());

        Assert.assertNotNull(ProtocolRegistry.getHandler("mysql"));
        Assert.assertNotNull(ProtocolRegistry.getHandler("sqlserver"));
        Assert.assertNotNull(ProtocolRegistry.getHandler("oracle"));
        Assert.assertNotNull(ProtocolRegistry.getHandler("postgresql"));
        Assert.assertNull(ProtocolRegistry.getHandler("unknown"));
    }

    @Test
    public void testMySQLDetection() {
        byte[] handshake = new byte[100];
        handshake[0] = 74;
        handshake[1] = 0;
        handshake[2] = 0;
        handshake[3] = 1;
        handshake[4] = 0x0A;
        handshake[5] = 0x35;
        handshake[6] = 0x2E;
        handshake[7] = 0x37;

        Buffer buf = Buffer.buffer(handshake);
        String protocol = ProtocolDetector.detect(buf);
        Assert.assertEquals("mysql", protocol);
    }

    @Test
    public void testTDSDetection() {
        byte[] prelogin = new byte[100];
        prelogin[0] = 0x12;
        Buffer buf = Buffer.buffer(prelogin);
        String protocol = ProtocolDetector.detect(buf);
        Assert.assertEquals("sqlserver", protocol);
    }

    @Test
    public void testTDSLoginDetection() {
        byte[] login7 = new byte[100];
        login7[0] = 0x10;
        Buffer buf = Buffer.buffer(login7);
        String protocol = ProtocolDetector.detect(buf);
        Assert.assertEquals("sqlserver", protocol);
    }

    @Test
    public void testTNSDetection() {
        byte[] tnsConnect = new byte[20];
        tnsConnect[0] = 0x00;
        tnsConnect[1] = 0x50;
        tnsConnect[4] = 0x01;
        Buffer buf = Buffer.buffer(tnsConnect);
        String protocol = ProtocolDetector.detect(buf);
        Assert.assertEquals("oracle", protocol);
    }

    @Test
    public void testPGDetection() {
        byte[] pgStartup = new byte[20];
        pgStartup[0] = 0x00;
        pgStartup[1] = 0x00;
        pgStartup[2] = 0x00;
        pgStartup[3] = 0x28;
        pgStartup[4] = 0x00;
        pgStartup[5] = 0x03;
        pgStartup[6] = 0x00;
        pgStartup[7] = 0x00;
        Buffer buf = Buffer.buffer(pgStartup);
        String protocol = ProtocolDetector.detect(buf);
        Assert.assertEquals("postgresql", protocol);
    }

    @Test
    public void testUnknownDetection() {
        byte[] unknown = new byte[10];
        Buffer buf = Buffer.buffer(unknown);
        String protocol = ProtocolDetector.detect(buf);
        Assert.assertNull(protocol);
    }

    @Test
    public void testNullDetection() {
        Assert.assertNull(ProtocolDetector.detect(null));
    }

    @Test
    public void testShortBufferDetection() {
        byte[] shortBuf = new byte[2];
        Buffer buf = Buffer.buffer(shortBuf);
        Assert.assertNull(ProtocolDetector.detect(buf));
    }
}