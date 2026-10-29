# connectivity-site-gateway

The on-premises gateway: it sits at a customer site between the devices and the cloud, the way a support gateway does in real fleets. Written in Java 21, deliberately different from the Go services, so failure drills include JVM-specific problems.

Part of [Connectivity](https://github.com/spr732/connectivity-platform), a mock edge-to-cloud telemetry system built for practicing production debugging.

## What it does

- **Transparent to devices.** It serves the same `/v1` API as the cloud. A device switches to gateway mode just by setting `INGEST_URL` to the gateway.
- **Store-and-forward for telemetry.** While the cloud is healthy and nothing is queued, telemetry goes straight through and the cloud's answer goes back to the device. If the cloud fails (5xx, timeout, unreachable), or a backlog already exists, the batch is written to a disk queue and the device gets `202 {"queued": true}`. One forwarder thread drains the queue oldest first, so delivery order matches arrival order.
- **Proxy for everything else.** Registration, heartbeats and log bundle chunks pass through, because devices already retry them and bundles have their own spool at the edge. If the cloud doesn't answer, the device gets a 503 with `Retry-After`.
- **Site inventory.** Every device the gateway hears from is tracked as online, stale or offline, and saved to disk. See `GET /gateway/v1/devices`.

## Failure behavior

| Situation | What the gateway does |
|---|---|
| Cloud returns 5xx, times out, or is unreachable | Telemetry: queues it and acks the device. Other routes: 503 to the device. |
| Cloud returns 4xx | Passes it back; the device decides (it drops permanent rejections). |
| Cloud rejects a queued batch (4xx) | Drops that one batch and counts it, so it can't block the queue. |
| Queue full (`QUEUE_MAX_RECORDS` / `QUEUE_MAX_BYTES`) | Evicts the oldest batches and counts them. This is data loss; it is alerted on. |
| Too many concurrent cloud calls | Waits up to `UPSTREAM_ACQUIRE_TIMEOUT` for a slot, then treats the cloud as busy. |
| All HTTP workers busy and their queue full | Drops the connection and counts it (`connectivity_gateway_http_rejected_total`). |
| Gateway restart | Queue and inventory are on disk; draining resumes where it stopped. |

Readiness (`/readyz`) checks only that the queue directory is writable, never the cloud. A gateway whose readiness followed the cloud would be pulled out of service at exactly the moment it is needed.

## Where things are deliberately simple

Each of these is a design choice worth knowing during an incident:

- **Plain JDK.** The HTTP server is `com.sun.net.httpserver` on a fixed thread pool with a bounded queue, like a servlet container's connector. The HTTP client is `java.net.http`. Jackson is the only runtime dependency.
- **Metrics are a ~200-line registry** (`MetricsRegistry`) and JVM metrics come straight from the MXBeans (`JvmMetrics`), so every number on the dashboard can be traced to one line of code.
- **The forwarder is a single thread.** That preserves order and paces a recovering cloud, at the cost of drain throughput. Watch `connectivity_gateway_queue_wait_seconds` after an outage.
- **Synchronous pass-through holds an HTTP worker for the whole cloud round trip.** When the cloud gets slow rather than down, that is where threads go.

## Endpoints

| Route | Purpose |
|---|---|
| `/v1/*` | Same API as the cloud ingest API |
| `GET /gateway/v1/devices` | Site inventory with online/stale/offline status |
| `GET /healthz` | Liveness |
| `GET /readyz` | Readiness: the queue is writable |
| `GET /metrics` | Prometheus metrics: `connectivity_gateway_*` and `jvm_*` |

## Configuration

| Variable | Default |
|---|---|
| `PORT` | `8090` |
| `UPSTREAM_URL` | `http://localhost:8080` |
| `GATEWAY_ID` | `site-gateway` |
| `DATA_DIR` | `/var/lib/connectivity-gateway` (queue and inventory) |
| `HTTP_THREADS` / `HTTP_QUEUE` | `32` / `256` |
| `MAX_BODY_BYTES` | `2097152` |
| `UPSTREAM_TIMEOUT` / `UPSTREAM_CONNECT_TIMEOUT` | `10s` / `3s` |
| `UPSTREAM_MAX_CONCURRENCY` / `UPSTREAM_ACQUIRE_TIMEOUT` | `16` / `2s` |
| `QUEUE_MAX_RECORDS` / `QUEUE_MAX_BYTES` | `100000` / `536870912` |
| `DEVICE_STALE_AFTER` / `DEVICE_OFFLINE_AFTER` | `1m` / `5m` |
| `JAVA_OPTS` | `-Xms64m -Xmx256m -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError` |
| `LOG_LEVEL` | `info` |

Durations use Go syntax (`500ms`, `10s`, `1m30s`), like every other service in the stack.

## Build and test

```bash
mvn verify                              # compile with -Werror, run tests, build target/site-gateway.jar
docker build -t connectivity/site-gateway .
```
