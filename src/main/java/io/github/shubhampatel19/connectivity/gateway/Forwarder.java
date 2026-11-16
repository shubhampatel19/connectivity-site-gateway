package io.github.shubhampatel19.connectivity.gateway;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Drains the store-and-forward queue to the cloud, oldest first, on one
 * thread so delivery order matches arrival order. A transient failure pauses
 * draining with exponential backoff; the head record is retried, never
 * skipped. A record the cloud rejects outright is dropped and counted, so one
 * bad batch can't block the queue forever.
 *
 * <p>A 401 or 403 is the exception to "rejected outright". It says the
 * credentials are wrong, not the record, and credentials change: the cloud's
 * signing secret is rotated, or a deploy ships the wrong one. Dropping on a
 * 401 would throw away the whole backlog the moment that happens. So the
 * forwarder first retries with the newest credentials the device has
 * presented, and if those are refused too it holds the queue. Only when
 * nothing has been delivered for {@code authRejectGrace} does it give up on
 * the record, so one device with a permanently bad token can't hold up a
 * site forever.
 */
public final class Forwarder implements Runnable {
    private final DiskQueue queue;
    private final UpstreamClient upstream;
    private final GatewayMetrics metrics;
    private final Log log;
    private final Clock clock;
    private final Duration baseBackoff;
    private final Duration maxBackoff;
    private final Credentials credentials;
    private final Duration authRejectGrace;
    /** When the current run of credential refusals began; -1 when the last delivery succeeded. */
    private long authRefusedSince = -1;
    private final Object signal = new Object();
    private volatile boolean running = true;
    private Thread thread;

    public Forwarder(DiskQueue queue, UpstreamClient upstream, GatewayMetrics metrics, Log log, Clock clock,
                     Duration baseBackoff, Duration maxBackoff) {
        this(queue, upstream, metrics, log, clock, baseBackoff, maxBackoff, new Credentials(), Duration.ofMinutes(30));
    }

    public Forwarder(DiskQueue queue, UpstreamClient upstream, GatewayMetrics metrics, Log log, Clock clock,
                     Duration baseBackoff, Duration maxBackoff, Credentials credentials, Duration authRejectGrace) {
        this.credentials = credentials;
        this.authRejectGrace = authRejectGrace;
        this.queue = queue;
        this.upstream = upstream;
        this.metrics = metrics;
        this.log = log;
        this.clock = clock;
        this.baseBackoff = baseBackoff;
        this.maxBackoff = maxBackoff;
    }

    public void start() {
        thread = new Thread(this, "forwarder");
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {
        running = false;
        synchronized (signal) {
            signal.notifyAll();
        }
        if (thread != null) {
            try {
                thread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void run() {
        int failures = 0;
        while (running) {
            List<DiskQueue.Entry> batch = queue.peek(100);
            if (batch.isEmpty()) {
                pause(Duration.ofMillis(250));
                continue;
            }
            boolean healthy = true;
            for (DiskQueue.Entry e : batch) {
                if (!running) {
                    return;
                }
                if (!forward(e)) {
                    healthy = false;
                    break;
                }
            }
            if (healthy) {
                if (failures > 0) {
                    log.info("cloud reachable again, draining queue", "queued", queue.size());
                }
                failures = 0;
                continue;
            }
            failures++;
            pause(backoff(failures));
        }
    }

    /** Delivers one record. Returns false when draining should pause. */
    boolean forward(DiskQueue.Entry e) {
        DiskQueue.Item item;
        try {
            item = queue.read(e);
        } catch (IOException ex) {
            queue.remove(e);
            metrics.dropped.labels("corrupt").add(1);
            log.error("discarding unreadable queue record", "path", e.path().toString(), "err", ex);
            return true;
        }
        if (item == null) {
            return true; // evicted while we were working
        }

        String kind = item.kind();
        String auth = credentials.latest(item.deviceId(), item.authorization());
        Map<String, String> headers = new HashMap<>(Map.of(
                "content-type", "application/json",
                "x-device-id", item.deviceId(),
                "authorization", auth,
                // How long the record waited here, as a duration rather than a
                // timestamp, so a skewed site clock can't distort it.
                "x-gateway-queued-ms", Long.toString(Math.max(0, clock.millis() - item.receivedAt()))));
        if (item.traceparent() != null) {
            headers.put("traceparent", item.traceparent());
        }
        byte[] body = item.body().getBytes(StandardCharsets.UTF_8);
        UpstreamClient.Response r = upstream.send(kind + "_forward", "POST", item.target(), headers, body);
        if (authRefused(r) && !auth.equals(item.authorization())) {
            // The newer credentials were refused; the record's own may still
            // be good (something else on the site LAN can claim a device ID).
            headers.put("authorization", item.authorization());
            r = upstream.send(kind + "_forward", "POST", item.target(), headers, body);
        }

        if (r.outcome() == UpstreamClient.Outcome.OK) {
            authRefusedSince = -1;
            queue.remove(e);
            metrics.forwarded.labels(kind).add(1);
            metrics.queueWait.observe(Math.max(0, clock.millis() - item.receivedAt()) / 1000.0);
            return true;
        }
        if (authRefused(r)) {
            long now = clock.millis();
            if (authRefusedSince < 0) {
                authRefusedSince = now;
            }
            if (now - authRefusedSince < authRejectGrace.toMillis()) {
                metrics.forwardFailures.labels("http_" + r.status()).add(1);
                log.warn("cloud refused a queued record's credentials; holding the queue, not dropping it",
                        "kind", kind, "status", r.status(), "device_id", item.deviceId(), "id", item.batchId(),
                        "queued", queue.size(), "held_ms", now - authRefusedSince);
                return false;
            }
            // Refused for the whole grace period: give up on this record.
        }
        if (!r.transientFailure()) {
            queue.remove(e);
            metrics.dropped.labels("rejected").add(1);
            log.warn("cloud rejected a queued record, dropping it",
                    "kind", kind, "status", r.status(), "device_id", item.deviceId(), "id", item.batchId());
            return true;
        }
        String reason = r.outcome() == UpstreamClient.Outcome.CLIENT_ERROR ? "http_" + r.status()
                : r.outcome().name().toLowerCase(Locale.ROOT);
        metrics.forwardFailures.labels(reason).add(1);
        log.warn("cloud unavailable, pausing queue drain", "reason", reason, "queued", queue.size());
        return false;
    }

    private static boolean authRefused(UpstreamClient.Response r) {
        return r.outcome() == UpstreamClient.Outcome.CLIENT_ERROR && (r.status() == 401 || r.status() == 403);
    }

    /** Exponential backoff with equal jitter. */
    Duration backoff(int failures) {
        long ceiling = maxBackoff.toMillis();
        long d = baseBackoff.toMillis() << Math.min(failures - 1, 20);
        if (d <= 0 || d > ceiling) {
            d = ceiling;
        }
        long half = d / 2;
        return Duration.ofMillis(half + ThreadLocalRandom.current().nextLong(half + 1));
    }

    private void pause(Duration d) {
        synchronized (signal) {
            try {
                signal.wait(Math.max(1, d.toMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            }
        }
    }
}
