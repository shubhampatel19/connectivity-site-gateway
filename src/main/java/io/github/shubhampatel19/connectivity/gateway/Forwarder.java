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
 */
public final class Forwarder implements Runnable {
    private final DiskQueue queue;
    private final UpstreamClient upstream;
    private final GatewayMetrics metrics;
    private final Log log;
    private final Clock clock;
    private final Duration baseBackoff;
    private final Duration maxBackoff;
    private final Object signal = new Object();
    private volatile boolean running = true;
    private Thread thread;

    public Forwarder(DiskQueue queue, UpstreamClient upstream, GatewayMetrics metrics, Log log, Clock clock,
                     Duration baseBackoff, Duration maxBackoff) {
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
        Map<String, String> headers = new HashMap<>(Map.of(
                "content-type", "application/json",
                "x-device-id", item.deviceId(),
                "authorization", item.authorization(),
                // How long the record waited here, as a duration rather than a
                // timestamp, so a skewed site clock can't distort it.
                "x-gateway-queued-ms", Long.toString(Math.max(0, clock.millis() - item.receivedAt()))));
        if (item.traceparent() != null) {
            headers.put("traceparent", item.traceparent());
        }
        UpstreamClient.Response r = upstream.send(kind + "_forward", "POST", item.target(), headers,
                item.body().getBytes(StandardCharsets.UTF_8));

        if (r.outcome() == UpstreamClient.Outcome.OK) {
            queue.remove(e);
            metrics.forwarded.labels(kind).add(1);
            metrics.queueWait.observe(Math.max(0, clock.millis() - item.receivedAt()) / 1000.0);
            return true;
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
