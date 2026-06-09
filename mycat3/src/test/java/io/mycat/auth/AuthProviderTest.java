package io.mycat.auth;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class AuthProviderTest {

    private NativePasswordAuth nativeAuth;
    private SCRAMSHA256Auth scramAuth;
    private AuthProviderRegistry registry;

    @Before
    public void setUp() {
        nativeAuth = new NativePasswordAuth();
        scramAuth = new SCRAMSHA256Auth();
        registry = new AuthProviderRegistry();
    }

    @Test
    public void testAuthResultSuccess() {
        AuthProvider.AuthResult result = new AuthProvider.AuthResult(true, "testdb", Map.of("user", "test", "host", "%"));
        Assert.assertTrue(result.isSuccess());
        Assert.assertEquals("testdb", result.getDatabase());
        Assert.assertEquals("test", result.getProperties().get("user"));
        Assert.assertEquals("%", result.getProperties().get("host"));
    }

    @Test
    public void testAuthResultFailure() {
        AuthProvider.AuthResult result = new AuthProvider.AuthResult(false, null, null);
        Assert.assertFalse(result.isSuccess());
        Assert.assertNull(result.getDatabase());
        Assert.assertTrue(result.getProperties().isEmpty());
    }

    @Test
    public void testNativePasswordAuthMethod() {
        Assert.assertEquals("mysql_native_password", nativeAuth.authMethod());
    }

    @Test
    public void testNativePasswordAuthPriority() {
        Assert.assertEquals(10, nativeAuth.priority());
    }

    @Test
    public void testNativePasswordAuthChallenge() {
        CompletableFuture<byte[]> challenge = nativeAuth.generateChallenge("testuser");
        Assert.assertNotNull(challenge);
        byte[] data = challenge.join();
        Assert.assertNotNull(data);
    }

    @Test
    public void testNativePasswordAuthAuthenticate() {
        byte[] challenge = "test".getBytes();
        byte[] response = "test".getBytes();
        CompletableFuture<AuthProvider.AuthResult> future = nativeAuth.authenticate("testuser", challenge, response);
        Assert.assertNotNull(future);
        AuthProvider.AuthResult result = future.join();
        Assert.assertTrue(result.isSuccess());
        Assert.assertEquals("default", result.getDatabase());
    }

    @Test
    public void testSCRAMSHA256AuthMethod() {
        Assert.assertEquals("scram-sha-256", scramAuth.authMethod());
    }

    @Test
    public void testSCRAMSHA256AuthAuthenticate() {
        byte[] challenge = "test".getBytes();
        byte[] response = "test".getBytes();
        CompletableFuture<AuthProvider.AuthResult> future = scramAuth.authenticate("testuser", challenge, response);
        Assert.assertNotNull(future);
        AuthProvider.AuthResult result = future.join();
        Assert.assertTrue(result.isSuccess());
        Assert.assertEquals("postgres", result.getDatabase());
    }

    @Test
    public void testRegistryRegisterAndGet() {
        registry.register(nativeAuth);
        Optional<AuthProvider> provider = registry.getProvider("mysql_native_password");
        Assert.assertTrue(provider.isPresent());
        Assert.assertEquals("mysql_native_password", provider.get().authMethod());
    }

    @Test
    public void testRegistryUnsupportedMethod() {
        Optional<AuthProvider> provider = registry.getProvider("unknown_method");
        Assert.assertFalse(provider.isPresent());
    }

    @Test
    public void testRegistryGetAllProviders() {
        registry.register(nativeAuth);
        registry.register(scramAuth);
        List<AuthProvider> providers = registry.getAllProviders();
        Assert.assertEquals(2, providers.size());
    }

    @Test
    public void testRegistryGetSupportedMethods() {
        registry.register(nativeAuth);
        registry.register(scramAuth);
        List<String> methods = registry.getSupportedMethods();
        Assert.assertTrue(methods.contains("mysql_native_password"));
        Assert.assertTrue(methods.contains("scram-sha-256"));
    }
}