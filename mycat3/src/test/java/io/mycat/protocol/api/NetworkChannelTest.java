package io.mycat.protocol.api;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import org.junit.Assert;
import org.junit.Test;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NetworkChannelTest {

    @Test
    public void testVertxNetworkChannel() throws Exception {
        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);

        NetServer server = vertx.createNetServer(
                new NetServerOptions().setPort(19999));
        server.connectHandler(socket -> {
            VertxNetworkChannel channel = new VertxNetworkChannel(socket);
            Assert.assertNotNull(channel.id());
            Assert.assertNotNull(channel.remoteAddress());
            channel.close();
            latch.countDown();
        });

        server.listen(result -> {
            if (result.succeeded()) {
                NetClient client = vertx.createNetClient();
                client.connect(19999, "localhost", ar -> {
                    if (ar.succeeded()) {
                        ar.result().close();
                    }
                });
            }
        });

        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        server.close(v -> vertx.close());
    }

    @Test
    public void testVertxNetworkChannelWrite() throws Exception {
        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);

        NetServer server = vertx.createNetServer(
                new NetServerOptions().setPort(19998));
        server.connectHandler(socket -> {
            socket.handler(buf -> {
                Assert.assertEquals("hello", buf.toString());
                latch.countDown();
            });
        });

        server.listen(result -> {
            if (result.succeeded()) {
                NetClient client = vertx.createNetClient();
                client.connect(19998, "localhost", ar -> {
                    if (ar.succeeded()) {
                        NetSocket clientSocket = ar.result();
                        VertxNetworkChannel channel = new VertxNetworkChannel(clientSocket);
                        channel.write(Buffer.buffer("hello"));
                    }
                });
            }
        });

        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        server.close(v -> vertx.close());
    }
}