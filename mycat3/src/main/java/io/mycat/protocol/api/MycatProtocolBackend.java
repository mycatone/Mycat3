package io.mycat.protocol.api;

import io.mycat.Authenticator;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.MycatUser;
import io.mycat.Response;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.commands.MycatdbCommand;
import io.mycat.config.UserConfig;
import io.mycat.runtime.MycatDataContextImpl;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.function.IntFunction;

/**
 * Shared facade used by the non-MySQL front-end protocols (TDS / TNS / PG) so they
 * can authenticate against the real {@link Authenticator} and run SQL through the
 * real Mycat sharding/routing pipeline ({@link MycatdbCommand}) instead of any
 * standalone backend.
 */
public final class MycatProtocolBackend {

    private static final Logger LOGGER = LoggerFactory.getLogger(MycatProtocolBackend.class);

    public UserConfig authenticate(String username, String host, String password) {
        Authenticator authenticator = tryGet(Authenticator.class);
        if (authenticator == null) {
            UserConfig anonymous = new UserConfig();
            anonymous.setUsername(username);
            return anonymous;
        }
        Authenticator.AuthInfo info = authenticator.getPassword(username, host);
        if (info == null || !info.isOk()) {
            LOGGER.info("auth rejected for user={} host={}: {}",
                    username, host, info == null ? "no AuthInfo" : info.getException());
            return null;
        }
        String expected = info.getRightPassword();
        if (expected != null && !expected.isEmpty() && !expected.equals(password)) {
            LOGGER.info("auth password mismatch for user={} host={} password={} expected={}", username, host,password,expected);
            return null;
        }
        return authenticator.getUserInfo(username);
    }

    /** Returns the configured plaintext password for {@code username}, or {@code null} if user is unknown. */
    public String lookupStoredPassword(String username, String host) {
        Authenticator authenticator = tryGet(Authenticator.class);
        if (authenticator == null) return "";
        Authenticator.AuthInfo info = authenticator.getPassword(username, host);
        if (info == null || !info.isOk()) return null;
        return info.getRightPassword() == null ? "" : info.getRightPassword();
    }

    /** Returns the {@link UserConfig} for {@code username} without checking password. Use after a protocol-specific auth check has succeeded. */
    public UserConfig lookupUserConfig(String username) {
        Authenticator authenticator = tryGet(Authenticator.class);
        if (authenticator == null) {
            UserConfig anonymous = new UserConfig();
            anonymous.setUsername(username);
            return anonymous;
        }
        return authenticator.getUserInfo(username);
    }

    public MycatDataContext createContext(String username,
                                          String host,
                                          InetSocketAddress remoteAddress,
                                          UserConfig userConfig,
                                          String requestedSchema) {
        MycatDataContext ctx = new MycatDataContextImpl();
        ctx.setUser(new MycatUser(username, null, null, host, remoteAddress, userConfig));
        ctx.useShcema(Optional.ofNullable(requestedSchema)
                .filter(s -> !s.isEmpty())
                .orElseGet(() -> userConfig == null ? null : userConfig.getSchema()));
        ctx.setAutoCommit(true);
        ctx.setIsolation(MySQLIsolation.REPEATED_READ);
        return ctx;
    }

    /**
     * Run {@code sql} through the full Mycat SQL pipeline. The given
     * {@code responseFactory} is invoked exactly once by {@code MycatdbCommand}
     * with the parsed-statement count; the resulting {@link Response} drains
     * the pipeline directly to the front-end socket via its protocol-specific
     * byte writers.
     */
    public Future<Void> executeSql(String sql,
                                   MycatDataContext ctx,
                                   IntFunction<Response> responseFactory) {
        if (sql == null || sql.trim().isEmpty()) {
            Response r = responseFactory.apply(1);
            return r.sendOk();
        }
        return MycatdbCommand.INSTANCE.executeQuery(sql, ctx, responseFactory);
    }

    private static <T> T tryGet(Class<T> clazz) {
        try {
            return MetaClusterCurrent.wrapper(clazz);
        } catch (Throwable ignored) {
            return null;
        }
    }
}
