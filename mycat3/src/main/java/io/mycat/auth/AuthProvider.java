package io.mycat.auth;

import io.mycat.plugin.Plugin;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface AuthProvider extends Plugin {
    String authMethod();
    int priority();
    CompletableFuture<byte[]> generateChallenge(String username);
    CompletableFuture<AuthResult> authenticate(String username, byte[] challenge, byte[] response);

    class AuthResult {
        private final boolean success;
        private final String database;
        private final Map<String, String> properties;

        public AuthResult(boolean success, String database, Map<String, String> properties) {
            this.success = success;
            this.database = database;
            this.properties = properties != null
                    ? Collections.unmodifiableMap(properties)
                    : Collections.emptyMap();
        }

        public boolean isSuccess() {
            return success;
        }

        public String getDatabase() {
            return database;
        }

        public Map<String, String> getProperties() {
            return properties;
        }
    }
}