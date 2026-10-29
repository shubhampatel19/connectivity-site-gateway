package io.github.shubhampatel19.connectivity.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Routes device traffic. The gateway serves the same /v1 API as the cloud,
 * so a device switches to gateway mode just by pointing at it.
 *
 * <ul>
 *   <li>Telemetry and alerts are store-and-forward: sent straight through
 *       while the cloud is healthy and the queue is empty, otherwise queued on
 *       disk and acked. They share one queue, so delivery order is arrival
 *       order across both.</li>
 *   <li>Everything else (registration, heartbeats, log bundle chunks) is
 *       proxied. Devices already retry those, and bundles have their own
 *       spool at the edge.</li>
 *   <li>Every request updates the site inventory.</li>
 * </ul>
 */
final class GatewayHandler implements HttpHandler {
    private static final Pattern BUNDLE_STATUS = Pattern.compile("^/v1/bundles/[A-Za-z0-9_-]+$");
    private static final Pattern BUNDLE_CHUNK = Pattern.compile("^/v1/bundles/[A-Za-z0-9_-]+/chunks/\\d+$");
    private static final Pattern BUNDLE_COMPLETE = Pattern.compile("^/v1/bundles/[A-Za-z0-9_-]+/complete$");

    private final String gatewayId;
    private final long maxBodyBytes;
    private final UpstreamClient upstream;
    private final DiskQueue queue;
    private final Inventory inventory;
    private final MetricsRegistry registry;
    private final GatewayMetrics metrics;
    private final ObjectMapper json;
    private final Clock clock;
    private final Log log;

    GatewayHandler(String gatewayId, long maxBodyBytes, UpstreamClient upstream, DiskQueue queue, Inventory inventory,
                   MetricsRegistry registry, GatewayMetrics metrics, ObjectMapper json, Clock clock, Log log) {
        this.gatewayId = gatewayId;
        this.maxBodyBytes = maxBodyBytes;
        this.upstream = upstream;
        this.queue = queue;
        this.inventory = inventory;
        this.registry = registry;
        this.metrics = metrics;
        this.json = json;
        this.clock = clock;
        this.log = log;
    }

    private static final class BodyTooLarge extends Exception {
        private static final long serialVersionUID = 1L;
    }

    @Override
    public void handle(HttpExchange ex) {
        long start = System.nanoTime();
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();
        String route = route(method, path);
        int status = 500;
        try {
            status = switch (route) {
                case "register" -> register(ex);
                case "telemetry" -> storeAndForward(ex, route, DiskQueue.Item.TELEMETRY, "batch_id");
                case "alerts" -> storeAndForward(ex, route, DiskQueue.Item.ALERTS, "alert_id");
                case "heartbeat", "bundle_init", "bundle_status", "bundle_chunk", "bundle_complete" -> proxy(ex, route);
                case "devices" -> devices(ex);
                case "metrics" -> respond(ex, 200, "text/plain; version=0.0.4; charset=utf-8",
                        registry.scrape().getBytes(StandardCharsets.UTF_8), null);
                case "healthz" -> respond(ex, 200, null, new byte[0], null);
                // Readiness never depends on the cloud: accepting data while
                // the cloud is down is the whole point of a gateway.
                case "readyz" -> Files.isWritable(queue.dir())
                        ? respond(ex, 200, null, new byte[0], null)
                        : error(ex, 503, "queue directory not writable");
                default -> error(ex, 404, "no such route");
            };
        } catch (BodyTooLarge e) {
            status = safeError(ex, 413, "body exceeds " + maxBodyBytes + " bytes");
        } catch (Exception e) {
            log.error("request failed", "route", route, "err", e);
            status = safeError(ex, 500, "internal error");
        } finally {
            ex.close();
            double seconds = (System.nanoTime() - start) / 1e9;
            metrics.requests.labels(route, Integer.toString(status)).add(1);
            metrics.requestDuration.labels(route).observe(seconds);
            Log.Level level = status >= 500 ? Log.Level.WARN : status >= 400 ? Log.Level.INFO : Log.Level.DEBUG;
            if (log.enabled(level) && !route.equals("metrics")) {
                log.log(level, "request", "route", route, "status", status,
                        "duration_ms", Math.round(seconds * 1000), "device_id", ex.getRequestHeaders().getFirst("X-Device-ID"));
            }
        }
    }

    static String route(String method, String path) {
        return switch (method + " " + path) {
            case "POST /v1/register" -> "register";
            case "POST /v1/heartbeat" -> "heartbeat";
            case "POST /v1/telemetry" -> "telemetry";
            case "POST /v1/alerts" -> "alerts";
            case "POST /v1/bundles" -> "bundle_init";
            case "GET /gateway/v1/devices" -> "devices";
            case "GET /metrics" -> "metrics";
            case "GET /healthz" -> "healthz";
            case "GET /readyz" -> "readyz";
            default -> {
                if (method.equals("GET") && BUNDLE_STATUS.matcher(path).matches()) {
                    yield "bundle_status";
                }
                if (method.equals("PUT") && BUNDLE_CHUNK.matcher(path).matches()) {
                    yield "bundle_chunk";
                }
                if (method.equals("POST") && BUNDLE_COMPLETE.matcher(path).matches()) {
                    yield "bundle_complete";
                }
                yield "not_found";
            }
        };
    }

    private int register(HttpExchange ex) throws IOException, BodyTooLarge {
        byte[] body = readBody(ex);
        UpstreamClient.Response r = upstream.send("register", "POST", "/v1/register", headers(ex), body);
        if (r.outcome() == UpstreamClient.Outcome.OK) {
            try {
                JsonNode req = json.readTree(body);
                inventory.registered(text(req, "device_id"), text(req, "model"), text(req, "firmware"), text(req, "site_id"));
            } catch (IOException e) {
                log.warn("cloud accepted a registration the gateway couldn't parse", "err", e);
            }
        }
        return relay(ex, r);
    }

    /**
     * Telemetry and alerts: the gateway's job is to not lose them. An alert
     * matters more than any one telemetry batch, because it is what opens a
     * support case, so it gets the same durability.
     */
    private int storeAndForward(HttpExchange ex, String route, String path, String idField) throws IOException, BodyTooLarge {
        String deviceId = ex.getRequestHeaders().getFirst("X-Device-ID");
        String auth = ex.getRequestHeaders().getFirst("Authorization");
        if (deviceId == null || auth == null) {
            return error(ex, 401, "missing device credentials");
        }
        byte[] body = readBody(ex);
        String recordId;
        try {
            recordId = text(json.readTree(body), idField);
        } catch (IOException e) {
            return error(ex, 400, "malformed JSON");
        }
        inventory.seen(deviceId);

        // Send straight through only when nothing is waiting: otherwise a new
        // batch would overtake older ones, and a struggling cloud would get
        // hammered by every device instead of one paced forwarder.
        if (queue.isEmpty()) {
            UpstreamClient.Response r = upstream.send(route, "POST", path, headers(ex), body);
            if (!r.transientFailure()) {
                return relay(ex, r); // delivered, or rejected for good: the device decides
            }
        }

        int evicted = queue.append(new DiskQueue.Item(deviceId, auth, recordId, clock.millis(),
                new String(body, StandardCharsets.UTF_8), path));
        metrics.queued.labels(route).add(1);
        if (evicted > 0) {
            metrics.dropped.labels("evicted").add(evicted);
            log.warn("queue full, evicted oldest records", "evicted", evicted);
        }
        return respond(ex, 202, "application/json",
                ("{\"queued\":true,\"gateway_id\":\"" + gatewayId + "\"}").getBytes(StandardCharsets.UTF_8), null);
    }

    private int proxy(HttpExchange ex, String route) throws IOException, BodyTooLarge {
        String deviceId = ex.getRequestHeaders().getFirst("X-Device-ID");
        if (deviceId != null) {
            inventory.seen(deviceId);
        }
        byte[] body = readBody(ex);
        String path = ex.getRequestURI().getRawPath();
        return relay(ex, upstream.send(route, ex.getRequestMethod(), path, headers(ex), body));
    }

    private int devices(HttpExchange ex) throws IOException {
        Map<String, Object> resp = new HashMap<>();
        resp.put("gateway_id", gatewayId);
        resp.put("devices", inventory.list());
        return respond(ex, 200, "application/json", json.writeValueAsBytes(resp), null);
    }

    /** Passes the cloud's answer back to the device, or a 503 if the cloud never answered. */
    private int relay(HttpExchange ex, UpstreamClient.Response r) throws IOException {
        switch (r.outcome()) {
            case TIMEOUT, UNAVAILABLE, BUSY -> {
                return respond(ex, 503, "application/json",
                        ("{\"error\":\"cloud unreachable from gateway (" + r.outcome().name().toLowerCase(Locale.ROOT) + ")\"}")
                                .getBytes(StandardCharsets.UTF_8), "5");
            }
            default -> {
                return respond(ex, r.status(), r.contentType(), r.body(), r.retryAfter());
            }
        }
    }

    private Map<String, String> headers(HttpExchange ex) {
        Map<String, String> h = new HashMap<>();
        ex.getRequestHeaders().forEach((k, v) -> {
            if (!v.isEmpty()) {
                h.put(k.toLowerCase(Locale.ROOT), v.get(0));
            }
        });
        return h;
    }

    private byte[] readBody(HttpExchange ex) throws IOException, BodyTooLarge {
        try (InputStream in = ex.getRequestBody()) {
            byte[] body = in.readNBytes((int) Math.min(Integer.MAX_VALUE - 8, maxBodyBytes + 1));
            if (body.length > maxBodyBytes) {
                throw new BodyTooLarge();
            }
            return body;
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n == null ? null : n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private int error(HttpExchange ex, int status, String msg) throws IOException {
        return respond(ex, status, "application/json", json.writeValueAsBytes(Map.of("error", msg)), null);
    }

    private int safeError(HttpExchange ex, int status, String msg) {
        try {
            return error(ex, status, msg);
        } catch (IOException ignored) {
            return status; // headers already sent or client gone
        }
    }

    private static int respond(HttpExchange ex, int status, String contentType, byte[] body, String retryAfter)
            throws IOException {
        if (contentType != null) {
            ex.getResponseHeaders().set("Content-Type", contentType);
        }
        if (retryAfter != null) {
            ex.getResponseHeaders().set("Retry-After", retryAfter);
        }
        boolean empty = body == null || body.length == 0 || status == 204;
        ex.sendResponseHeaders(status, empty ? -1 : body.length);
        if (!empty) {
            try (OutputStream out = ex.getResponseBody()) {
                out.write(body);
            }
        }
        return status;
    }

    static List<String> routes() {
        return List.of("register", "heartbeat", "telemetry", "alerts", "bundle_init", "bundle_status", "bundle_chunk", "bundle_complete");
    }
}
