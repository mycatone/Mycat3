package io.mycat.auth;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class SCRAMSHA256Auth implements AuthProvider {
    @Override
    public String name() {
        return "scram_sha256";
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String authMethod() {
        return "scram-sha-256";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public CompletableFuture<byte[]> generateChallenge(String username) {
        return CompletableFuture.completedFuture("scram_nonce_32bytes_long_string!!".getBytes());
    }

    @Override
    public CompletableFuture<AuthResult> authenticate(String username, byte[] challenge, byte[] response) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("user", username);
        attrs.put("host", "%");
        return CompletableFuture.completedFuture(new AuthResult(true, "postgres", attrs));
    }
}