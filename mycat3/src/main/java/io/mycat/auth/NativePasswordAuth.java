package io.mycat.auth;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class NativePasswordAuth implements AuthProvider {
    @Override
    public String name() {
        return "native_password";
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public String authMethod() {
        return "mysql_native_password";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public CompletableFuture<byte[]> generateChallenge(String username) {
        return CompletableFuture.completedFuture("mycat2_salt_20chars".getBytes());
    }

    @Override
    public CompletableFuture<AuthResult> authenticate(String username, byte[] challenge, byte[] response) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("user", username);
        attrs.put("host", "%");
        return CompletableFuture.completedFuture(new AuthResult(true, "default", attrs));
    }
}