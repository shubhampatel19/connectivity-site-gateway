package io.github.spr732.connectivity.gateway;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs a real gateway against a fake cloud. */
class GatewayTest {
    @TempDir
    Path dir;

    private final MutableClock clock = new MutableClock(Instant.parse("2026-10-29T12:00:00Z"));
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private final ObjectMapper json = new ObjectMapper();
    private FakeCloud cloud;
    private Gateway gateway;

    @BeforeEach
    void start() throws IOException {
        cloud = new FakeCloud();
        gateway = startGateway(cloud.uri(), Map.of());
    }

    @AfterEach
    void stop() {
        gateway.stop();
        cloud.close();
    }

    private Gateway startGateway(URI upstream, Map<String, String> extra) throws IOException {
        Map<String, String> env = new HashMap<>(Map.of(
                "PORT", "0", "UPSTREAM_URL", upstream.toString(), "DATA_DIR", dir.toString(),
                "GATEWAY_ID", "gw-test", "UPSTREAM_TIMEOUT", "2s", "UPSTREAM_CONNECT_TIMEOUT", "1s"));
        env.putAll(extra);
        Log quiet = Log.to(new PrintStream(OutputStream.nullOutputStream()), "test", Log.Level.ERROR);
        return Gateway.start(Config.fromEnv(env), quiet, clock, Duration.ofMillis(20), Duration.ofMillis(100));
    }

    private HttpResponse<String> send(String method, String path, byte[] body, Map<String, String> headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + gateway.port() + path))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body));
        headers.forEach(b::header);
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static Map<String, String> device(String id) {
        return Map.of("X-Device-ID", id, "Authorization", "Bearer tok-" + id, "Content-Type", "application/json");
    }

    private HttpResponse<String> telemetry(String device, String batchId) throws Exception {
        String body = "{\"batch_id\":\"" + batchId + "\",\"device_id\":\"" + device
                + "\",\"samples\":[{\"metric\":\"cpu_temp_c\",\"value\":50,\"ts\":\"2026-10-29T12:00:00Z\"}]}";
        return send("POST", "/v1/telemetry", body.getBytes(StandardCharsets.UTF_8), device(device));
    }

    private static void waitUntil(BooleanSupplier cond, String what) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("timed out waiting for " + what);
    }

    private List<String> acceptedBatchIds() {
        return cloud.to("/v1/telemetry").stream()
                .filter(r -> r.status() == 202)
                .map(r -> {
                    try {
                        return json.readTree(r.body()).get("batch_id").asText();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
    }

    private String metrics() throws Exception {
        return send("GET", "/metrics", null, Map.of()).body();
    }

    @Test
    void registrationIsProxiedAndRecorded() throws Exception {
        String body = "{\"device_id\":\"dev-1\",\"model\":\"edge-server-e1\",\"firmware\":\"1.4.2\",\"site_id\":\"site-b\"}";
        HttpResponse<String> r = send("POST", "/v1/register", body.getBytes(StandardCharsets.UTF_8),
                Map.of("X-Enrollment-Key", "k", "Content-Type", "application/json"));

        assertEquals(200, r.statusCode());
        assertTrue(r.body().contains("\"token\":\"tok\""), "the cloud's token reaches the device");
        FakeCloud.Request up = cloud.to("/v1/register").get(0);
        assertEquals("k", up.headers().get("x-enrollment-key"));
        assertEquals("gw-test", up.headers().get("x-gateway-id"));

        JsonNode devices = json.readTree(send("GET", "/gateway/v1/devices", null, Map.of()).body()).get("devices");
        assertEquals("dev-1", devices.get(0).get("deviceId").asText());
        assertEquals("edge-server-e1", devices.get(0).get("model").asText());
        assertEquals("online", devices.get(0).get("status").asText());
    }

    @Test
    void telemetryGoesStraightThroughWhenCloudIsHealthy() throws Exception {
        HttpResponse<String> r = telemetry("dev-1", "b1");
        assertEquals(202, r.statusCode());
        assertFalse(r.body().contains("queued"), "answered by the cloud, not the queue");
        assertEquals(0, gateway.queue().size());
        assertEquals("Bearer tok-dev-1", cloud.to("/v1/telemetry").get(0).headers().get("authorization"));
    }

    @Test
    void telemetryIsQueuedDuringOutageAndForwardedInOrder() throws Exception {
        cloud.telemetryStatus = 503;
        assertTrue(telemetry("dev-1", "b1").body().contains("\"queued\":true"));
        assertTrue(telemetry("dev-1", "b2").body().contains("\"queued\":true"));
        assertTrue(gateway.queue().size() >= 1);

        cloud.telemetryStatus = 202; // the cloud recovers
        // A batch arriving while the backlog drains must queue behind it, not overtake it.
        telemetry("dev-1", "b3");
        waitUntil(() -> gateway.queue().isEmpty(), "the queue to drain");

        assertEquals(List.of("b1", "b2", "b3"), acceptedBatchIds());
        assertTrue(metrics().contains("connectivity_gateway_forwarded_total 3"));
    }

    @Test
    void cloudRejectionIsPassedBackToTheDevice() throws Exception {
        cloud.telemetryStatus = 400;
        assertEquals(400, telemetry("dev-1", "b1").statusCode());
        assertEquals(0, gateway.queue().size(), "a batch the cloud rejects must not be queued");
    }

    @Test
    void queuedBatchTheCloudRejectsIsDropped() throws Exception {
        cloud.telemetryStatus = 503;
        telemetry("dev-1", "b1");
        cloud.telemetryStatus = 400;
        waitUntil(() -> gateway.queue().isEmpty(), "the rejected batch to be dropped");
        assertTrue(metrics().contains("connectivity_gateway_queue_dropped_total{reason=\"rejected\"} 1"));
    }

    @Test
    void queueAndInventorySurviveRestart() throws Exception {
        cloud.telemetryStatus = 503;
        send("POST", "/v1/register", "{\"device_id\":\"dev-1\"}".getBytes(StandardCharsets.UTF_8), Map.of());
        telemetry("dev-1", "b1");
        telemetry("dev-1", "b2");
        gateway.stop();

        cloud.telemetryStatus = 202;
        gateway = startGateway(cloud.uri(), Map.of());
        waitUntil(() -> gateway.queue().isEmpty(), "the restored queue to drain");
        assertEquals(List.of("b1", "b2"), acceptedBatchIds());
        assertTrue(send("GET", "/gateway/v1/devices", null, Map.of()).body().contains("dev-1"));
    }

    @Test
    void bundleChunksAreProxiedByteForByte() throws Exception {
        cloud.otherStatus = 204;
        byte[] chunk = new byte[64 * 1024];
        ThreadLocalRandom.current().nextBytes(chunk);
        Map<String, String> h = new HashMap<>(device("dev-1"));
        h.put("X-Chunk-SHA256", "abc123");
        h.put("X-Site-Secret", "must-not-leave-the-site");

        HttpResponse<String> r = send("PUT", "/v1/bundles/b-1/chunks/0", chunk, h);

        assertEquals(204, r.statusCode());
        FakeCloud.Request up = cloud.to("/v1/bundles/b-1/chunks/0").get(0);
        assertArrayEquals(chunk, up.body());
        assertEquals("abc123", up.headers().get("x-chunk-sha256"));
        assertEquals(null, up.headers().get("x-site-secret"), "only allow-listed headers go upstream");
    }

    @Test
    void unreachableCloudIs503ForProxiedRoutes() throws Exception {
        cloud.close();
        HttpResponse<String> r = send("POST", "/v1/heartbeat", "{}".getBytes(StandardCharsets.UTF_8), device("dev-1"));
        assertEquals(503, r.statusCode());
        assertEquals("5", r.headers().firstValue("retry-after").orElse(null));
        assertTrue(r.body().contains("unavailable"));
    }

    @Test
    void deviceStatusAgesFromOnlineToOffline() throws Exception {
        send("POST", "/v1/heartbeat", "{}".getBytes(StandardCharsets.UTF_8), device("dev-1"));
        assertTrue(metrics().contains("connectivity_gateway_devices{status=\"online\"} 1"));
        clock.advance(Duration.ofMinutes(2));
        assertTrue(metrics().contains("connectivity_gateway_devices{status=\"stale\"} 1"));
        clock.advance(Duration.ofMinutes(10));
        assertTrue(metrics().contains("connectivity_gateway_devices{status=\"offline\"} 1"));
    }

    @Test
    void oversizedBodyIsRejected() throws Exception {
        gateway.stop();
        gateway = startGateway(cloud.uri(), Map.of("MAX_BODY_BYTES", "100"));
        assertEquals(413, send("POST", "/v1/telemetry", new byte[500], device("dev-1")).statusCode());
        assertTrue(cloud.to("/v1/telemetry").isEmpty());
    }

    @Test
    void metricsExposeJvmAndGatewaySeries() throws Exception {
        telemetry("dev-1", "b1");
        String m = metrics();
        for (String series : List.of("jvm_memory_used_bytes{area=\"heap\"}", "jvm_gc_collection_seconds_count",
                "jvm_threads_current", "connectivity_gateway_http_threads_active", "connectivity_gateway_queue_records",
                "connectivity_gateway_upstream_requests_total{route=\"telemetry\",outcome=\"ok\"} 1",
                "connectivity_gateway_http_requests_total{route=\"telemetry\",code=\"202\"} 1")) {
            assertTrue(m.contains(series), "missing " + series);
        }
        assertEquals(200, send("GET", "/readyz", null, Map.of()).statusCode());
        assertNotNull(m);
    }
}
