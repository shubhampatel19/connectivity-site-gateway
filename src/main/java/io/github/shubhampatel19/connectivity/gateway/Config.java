package io.github.shubhampatel19.connectivity.gateway;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Gateway settings, read from environment variables. Every invalid setting is reported at once. */
public record Config(
        int port,
        URI upstream,
        String gatewayId,
        Path dataDir,
        int httpThreads,
        int httpQueue,
        long maxBodyBytes,
        Duration upstreamTimeout,
        Duration upstreamConnectTimeout,
        int upstreamMaxConcurrency,
        Duration upstreamAcquireTimeout,
        long queueMaxRecords,
        long queueMaxBytes,
        Duration staleAfter,
        Duration offlineAfter) {

    public static Config fromEnv(Map<String, String> env) {
        Env e = new Env(env);
        Config c = new Config(
                e.integer("PORT", 8090),
                e.uri("UPSTREAM_URL", "http://localhost:8080"),
                e.string("GATEWAY_ID", "site-gateway"),
                Path.of(e.string("DATA_DIR", "/var/lib/connectivity-gateway")),
                e.integer("HTTP_THREADS", 32),
                e.integer("HTTP_QUEUE", 256),
                e.longValue("MAX_BODY_BYTES", 2L << 20),
                e.duration("UPSTREAM_TIMEOUT", Duration.ofSeconds(10)),
                e.duration("UPSTREAM_CONNECT_TIMEOUT", Duration.ofSeconds(3)),
                e.integer("UPSTREAM_MAX_CONCURRENCY", 16),
                e.duration("UPSTREAM_ACQUIRE_TIMEOUT", Duration.ofSeconds(2)),
                e.longValue("QUEUE_MAX_RECORDS", 100_000),
                e.longValue("QUEUE_MAX_BYTES", 512L << 20),
                e.duration("DEVICE_STALE_AFTER", Duration.ofMinutes(1)),
                e.duration("DEVICE_OFFLINE_AFTER", Duration.ofMinutes(5)));
        e.check(c.port >= 0 && c.port <= 65535, "PORT must be between 0 and 65535");
        e.check(c.httpThreads > 0, "HTTP_THREADS must be positive");
        e.check(c.httpQueue > 0, "HTTP_QUEUE must be positive");
        e.check(c.maxBodyBytes > 0, "MAX_BODY_BYTES must be positive");
        e.check(c.upstreamMaxConcurrency > 0, "UPSTREAM_MAX_CONCURRENCY must be positive");
        e.check(c.queueMaxRecords > 0, "QUEUE_MAX_RECORDS must be positive");
        e.check(c.queueMaxBytes > 0, "QUEUE_MAX_BYTES must be positive");
        e.check(c.offlineAfter.compareTo(c.staleAfter) > 0, "DEVICE_OFFLINE_AFTER must be longer than DEVICE_STALE_AFTER");
        e.throwIfInvalid();
        return c;
    }

    private static final Pattern DURATION_PART = Pattern.compile("(\\d+(?:\\.\\d+)?)(ns|us|µs|ms|s|m|h)");

    /**
     * Parses Go-style durations ("500ms", "10s", "1m30s") so every service in
     * the stack is configured the same way.
     */
    static Duration parseDuration(String s) {
        String v = s.trim();
        if (v.equals("0")) {
            return Duration.ZERO;
        }
        Matcher m = DURATION_PART.matcher(v);
        long nanos = 0;
        int end = 0;
        while (m.find()) {
            if (m.start() != end) {
                throw new IllegalArgumentException(s);
            }
            double n = Double.parseDouble(m.group(1));
            double unit = switch (m.group(2)) {
                case "ns" -> 1;
                case "us", "µs" -> 1e3;
                case "ms" -> 1e6;
                case "s" -> 1e9;
                case "m" -> 60e9;
                default -> 3600e9;
            };
            nanos += (long) (n * unit);
            end = m.end();
        }
        if (end == 0 || end != v.length()) {
            throw new IllegalArgumentException(s);
        }
        return Duration.ofNanos(nanos);
    }

    private static final class Env {
        private final Map<String, String> env;
        private final List<String> errors = new ArrayList<>();

        Env(Map<String, String> env) {
            this.env = env;
        }

        private String raw(String key) {
            String v = env.get(key);
            return v == null || v.isBlank() ? null : v.trim();
        }

        String string(String key, String def) {
            String v = raw(key);
            return v == null ? def : v;
        }

        int integer(String key, int def) {
            String v = raw(key);
            if (v == null) {
                return def;
            }
            try {
                return Integer.parseInt(v);
            } catch (NumberFormatException ex) {
                errors.add(key + ": \"" + v + "\" is not an integer");
                return def;
            }
        }

        long longValue(String key, long def) {
            String v = raw(key);
            if (v == null) {
                return def;
            }
            try {
                return Long.parseLong(v);
            } catch (NumberFormatException ex) {
                errors.add(key + ": \"" + v + "\" is not an integer");
                return def;
            }
        }

        Duration duration(String key, Duration def) {
            String v = raw(key);
            if (v == null) {
                return def;
            }
            try {
                return parseDuration(v);
            } catch (IllegalArgumentException ex) {
                errors.add(key + ": \"" + v + "\" is not a duration");
                return def;
            }
        }

        URI uri(String key, String def) {
            String v = string(key, def);
            try {
                URI u = URI.create(v);
                if (u.getScheme() == null || u.getHost() == null) {
                    throw new IllegalArgumentException();
                }
                return u;
            } catch (IllegalArgumentException ex) {
                errors.add(key + ": \"" + v + "\" is not an absolute URL");
                return URI.create(def);
            }
        }

        void check(boolean ok, String message) {
            if (!ok) {
                errors.add(message);
            }
        }

        void throwIfInvalid() {
            if (!errors.isEmpty()) {
                throw new IllegalArgumentException("invalid configuration: " + String.join("; ", errors));
            }
        }
    }
}
