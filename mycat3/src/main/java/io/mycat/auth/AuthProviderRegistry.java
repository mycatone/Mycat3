package io.mycat.auth;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AuthProviderRegistry {
    private final Map<String, AuthProvider> providers = new LinkedHashMap<>();

    public void register(AuthProvider provider) {
        providers.put(provider.authMethod(), provider);
    }

    public Optional<AuthProvider> getProvider(String method) {
        return Optional.ofNullable(providers.get(method));
    }

    public List<AuthProvider> getAllProviders() {
        return new ArrayList<>(providers.values());
    }

    public List<String> getSupportedMethods() {
        return new ArrayList<>(providers.keySet());
    }
}