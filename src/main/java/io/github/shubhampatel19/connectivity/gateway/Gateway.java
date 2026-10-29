package io.github.shubhampatel19.connectivity.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Wires the gateway together and owns its lifecycle. */
public final class Gateway {
    private final HttpServer server;
    private final ThreadPoolExecutor httpPool;
    private final ScheduledExecutorService scheduler;
    private final Forwarder forwarder;
    private final Inventory inventory;
    private final DiskQueue queue;
    private final Path inventoryFile;
    private final ObjectMapper json;
    private final Log log;

    private Gateway(HttpServer server, ThreadPoolExecutor httpPool, ScheduledExecutorService scheduler,
                    Forwarder forwarder, Inventory inventory, DiskQueue queue, Path inventoryFile, ObjectMapper json, Log log) {
        this.server = server;
        this.httpPool = httpPool;
        this.scheduler = scheduler;
        this.forwarder = forwarder;
        this.inventory = inventory;
        this.queue = queue;
        this.inventoryFile = inventoryFile;
        this.json = json;
        this.log = log;
    }

    public static Gateway start(Config cfg, Log log, Clock clock) throws IOException {
        return start(cfg, log, clock, Duration.ofSeconds(1), Duration.ofSeconds(30));
    }

    static Gateway start(Config cfg, Log log, Clock clock, Duration retryBase, Duration retryMax) throws IOException {
        ObjectMapper json = new ObjectMapper();
        MetricsRegistry registry = new MetricsRegistry();
        GatewayMetrics metrics = new GatewayMetrics(registry);
        JvmMetrics.register(registry);

        Files.createDirectories(cfg.dataDir());
        Path inventoryFile = cfg.dataDir().resolve("inventory.json");
        Inventory inventory = new Inventory(clock, cfg.staleAfter(), cfg.offlineAfter());
        inventory.load(inventoryFile, json);
        DiskQueue queue = new DiskQueue(cfg.dataDir().resolve("queue"), cfg.queueMaxRecords(), cfg.queueMaxBytes(), json);
        if (!queue.isEmpty()) {
            log.info("resuming store-and-forward queue", "records", queue.size(), "bytes", queue.bytes());
        }

        UpstreamClient upstream = new UpstreamClient(cfg.upstream(), cfg.gatewayId(), cfg.upstreamConnectTimeout(),
                cfg.upstreamTimeout(), cfg.upstreamMaxConcurrency(), cfg.upstreamAcquireTimeout(), metrics);
        Forwarder forwarder = new Forwarder(queue, upstream, metrics, log, clock, retryBase, retryMax);

        // A fixed pool with a bounded queue, like a servlet container's
        // connector. When both fill up, new connections are dropped and counted.
        AtomicInteger n = new AtomicInteger();
        ThreadPoolExecutor httpPool = new ThreadPoolExecutor(cfg.httpThreads(), cfg.httpThreads(), 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(cfg.httpQueue()),
                r -> {
                    Thread t = new Thread(r, "http-" + n.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                },
                (r, pool) -> {
                    metrics.httpRejected.inc();
                    throw new RejectedExecutionException("all " + pool.getMaximumPoolSize() + " HTTP workers busy and queue full");
                });

        registry.gauge("connectivity_gateway_http_threads_active", "HTTP workers busy with a request.", httpPool::getActiveCount);
        registry.gauge("connectivity_gateway_http_threads_max", "HTTP worker pool size.", httpPool::getMaximumPoolSize);
        registry.gauge("connectivity_gateway_http_queue_depth", "Requests waiting for a free HTTP worker.", () -> httpPool.getQueue().size());
        registry.gauge("connectivity_gateway_upstream_inflight", "Cloud requests in progress.", upstream::inFlight);
        registry.gauge("connectivity_gateway_upstream_max_concurrency", "Cap on concurrent cloud requests.", cfg::upstreamMaxConcurrency);
        registry.gauge("connectivity_gateway_queue_records", "Records (telemetry batches and alerts) waiting in the store-and-forward queue.", queue::size);
        registry.gauge("connectivity_gateway_queue_bytes", "Bytes waiting in the store-and-forward queue.", queue::bytes);
        registry.gaugeVec("connectivity_gateway_devices", "Devices in the site inventory, by status.", "status", inventory::countsByStatus);

        HttpServer server = HttpServer.create(new InetSocketAddress(cfg.port()), 0);
        server.createContext("/", new GatewayHandler(cfg.gatewayId(), cfg.maxBodyBytes(), upstream, queue, inventory,
                registry, metrics, json, clock, log));
        server.setExecutor(httpPool);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "inventory-saver");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                inventory.saveIfDirty(inventoryFile, json);
            } catch (IOException e) {
                log.error("saving inventory failed", "err", e);
            }
        }, 10, 10, TimeUnit.SECONDS);

        forwarder.start();
        server.start();
        log.info("gateway listening", "port", server.getAddress().getPort(), "upstream", cfg.upstream().toString(),
                "gateway_id", cfg.gatewayId(), "http_threads", cfg.httpThreads(), "devices_known", inventory.size());
        return new Gateway(server, httpPool, scheduler, forwarder, inventory, queue, inventoryFile, json, log);
    }

    public int port() {
        return server.getAddress().getPort();
    }

    DiskQueue queue() {
        return queue;
    }

    /** Stops taking requests, lets in-flight ones finish, and saves state. */
    public void stop() {
        log.info("gateway stopping");
        server.stop(2);
        httpPool.shutdown();
        forwarder.stop();
        scheduler.shutdown();
        try {
            inventory.saveIfDirty(inventoryFile, json);
        } catch (IOException e) {
            log.error("saving inventory on shutdown failed", "err", e);
        }
        log.info("gateway stopped", "queued", queue.size());
    }
}
