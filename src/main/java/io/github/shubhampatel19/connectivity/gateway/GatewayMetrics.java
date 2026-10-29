package io.github.shubhampatel19.connectivity.gateway;

/** The gateway's own metrics. JVM metrics are registered separately by {@link JvmMetrics}. */
final class GatewayMetrics {
    private static final double[] LATENCY = {.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10};
    private static final double[] QUEUE_WAIT = {.1, 1, 5, 10, 30, 60, 300, 900, 1800, 3600};

    final MetricsRegistry.Counter requests;
    final MetricsRegistry.Histogram requestDuration;
    final MetricsRegistry.Counter httpRejected;
    final MetricsRegistry.Counter upstreamRequests;
    final MetricsRegistry.Histogram upstreamDuration;
    final MetricsRegistry.Counter queued;
    final MetricsRegistry.Counter forwarded;
    final MetricsRegistry.Counter dropped;
    final MetricsRegistry.Counter forwardFailures;
    final MetricsRegistry.Histogram queueWait;

    GatewayMetrics(MetricsRegistry r) {
        requests = r.counter("connectivity_gateway_http_requests_total",
                "Device-facing requests handled, by route and status code.", "route", "code");
        requestDuration = r.histogram("connectivity_gateway_http_request_duration_seconds",
                "Device-facing request latency, by route.", LATENCY, "route");
        httpRejected = r.counter("connectivity_gateway_http_rejected_total",
                "Connections dropped because every HTTP worker was busy and the work queue was full.");
        upstreamRequests = r.counter("connectivity_gateway_upstream_requests_total",
                "Requests to the cloud, by route and outcome (ok, client_error, server_error, timeout, unavailable, busy).", "route", "outcome");
        upstreamDuration = r.histogram("connectivity_gateway_upstream_request_duration_seconds",
                "Cloud request latency, by route.", LATENCY, "route");
        queued = r.counter("connectivity_gateway_queued_total",
                "Records accepted into the store-and-forward queue, by kind (telemetry, alerts).", "kind");
        forwarded = r.counter("connectivity_gateway_forwarded_total",
                "Queued records delivered to the cloud, by kind (telemetry, alerts).", "kind");
        dropped = r.counter("connectivity_gateway_queue_dropped_total",
                "Queued records discarded, by reason (evicted, rejected, corrupt).", "reason");
        forwardFailures = r.counter("connectivity_gateway_forward_failures_total",
                "Forwarding attempts that failed and paused draining, by reason.", "reason");
        queueWait = r.histogram("connectivity_gateway_queue_wait_seconds",
                "Time a record spent in the queue before the cloud accepted it.", QUEUE_WAIT);
    }
}
