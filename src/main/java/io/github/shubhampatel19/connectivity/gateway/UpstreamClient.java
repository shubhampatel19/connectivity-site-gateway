package io.github.shubhampatel19.connectivity.gateway;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * The gateway's single connection pool to the cloud. A semaphore caps
 * concurrent upstream calls: when the cloud slows down, callers wait a
 * bounded time for a slot and then give up as BUSY, instead of piling up
 * unbounded threads on a slow link.
 */
public final class UpstreamClient {

    public enum Outcome { OK, CLIENT_ERROR, SERVER_ERROR, TIMEOUT, UNAVAILABLE, BUSY }

    public record Response(int status, String contentType, String retryAfter, byte[] body, Outcome outcome) {
        /** Whether trying again later could succeed. */
        public boolean transientFailure() {
            return switch (outcome) {
                case OK -> false;
                case CLIENT_ERROR -> status == 408 || status == 429;
                default -> true;
            };
        }
    }

    /**
     * Request headers passed to the cloud. Everything else stays at the site.
     * traceparent and tracestate carry the device's trace through the
     * gateway; x-gateway-queued-ms says how long a forwarded record waited.
     */
    static final Set<String> FORWARDED_HEADERS = Set.of(
            "content-type", "authorization", "x-device-id", "x-enrollment-key", "x-chunk-sha256", "x-request-id",
            "traceparent", "tracestate", "x-gateway-queued-ms");

    private final URI base;
    private final String gatewayId;
    private final Duration timeout;
    private final Duration acquireTimeout;
    private final int maxConcurrency;
    private final Semaphore permits;
    private final HttpClient client;
    private final GatewayMetrics metrics;

    public UpstreamClient(URI base, String gatewayId, Duration connectTimeout, Duration timeout,
                          int maxConcurrency, Duration acquireTimeout, GatewayMetrics metrics) {
        this.base = base;
        this.gatewayId = gatewayId;
        this.timeout = timeout;
        this.acquireTimeout = acquireTimeout;
        this.maxConcurrency = maxConcurrency;
        this.permits = new Semaphore(maxConcurrency);
        this.metrics = metrics;
        // HTTP/1.1 explicitly: otherwise the client tries an h2c upgrade on
        // every new cleartext connection.
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(connectTimeout)
                .build();
    }

    public Response send(String route, String method, String path, Map<String, String> headers, byte[] body) {
        long start = System.nanoTime();
        Response r = doSend(method, path, headers, body);
        metrics.upstreamRequests.labels(route, r.outcome().name().toLowerCase(Locale.ROOT)).add(1);
        metrics.upstreamDuration.labels(route).observe((System.nanoTime() - start) / 1e9);
        return r;
    }

    private Response doSend(String method, String path, Map<String, String> headers, byte[] body) {
        try {
            if (!permits.tryAcquire(acquireTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                return failure(Outcome.BUSY);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return failure(Outcome.UNAVAILABLE);
        }
        try {
            HttpRequest.BodyPublisher publisher = body == null || body.length == 0
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(body);
            HttpRequest.Builder b = HttpRequest.newBuilder(base.resolve(path)).timeout(timeout).method(method, publisher);
            headers.forEach((k, v) -> {
                if (v != null && FORWARDED_HEADERS.contains(k.toLowerCase(Locale.ROOT))) {
                    b.header(k, v);
                }
            });
            b.header("X-Gateway-ID", gatewayId);

            HttpResponse<byte[]> resp = client.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
            int status = resp.statusCode();
            Outcome outcome = status < 400 ? Outcome.OK : status < 500 ? Outcome.CLIENT_ERROR : Outcome.SERVER_ERROR;
            return new Response(status,
                    resp.headers().firstValue("content-type").orElse(null),
                    resp.headers().firstValue("retry-after").orElse(null),
                    resp.body(), outcome);
        } catch (HttpTimeoutException e) {
            return failure(Outcome.TIMEOUT);
        } catch (IOException e) {
            return failure(Outcome.UNAVAILABLE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return failure(Outcome.UNAVAILABLE);
        } finally {
            permits.release();
        }
    }

    private static Response failure(Outcome o) {
        return new Response(503, null, null, new byte[0], o);
    }

    /** Upstream calls in progress right now. */
    public int inFlight() {
        return maxConcurrency - permits.availablePermits();
    }
}
