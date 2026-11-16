# connectivity-site-gateway: design

The gateway runs at a customer's site, between that site's devices and the cloud. It speaks the cloud's own `/v1` API, so a device is pointed at it by changing one URL. Its reason to exist is one promise: **telemetry and alerts a device has handed over are not lost while the cloud is unreachable.**

It is written in Java 21 with no framework: the JDK's HTTP server and client, Jackson for JSON, and a small metrics registry of its own. That keeps the failure modes visible (thread pools, queues, heap, GC) instead of hidden inside a container.

## Layout

| File | Responsibility |
|---|---|
| `Main.java` | Reads configuration, starts the gateway, handles shutdown |
| `Config.java` | Settings from environment variables, every problem reported at once |
| `Gateway.java` | Wires everything: HTTP server and worker pool, queue, forwarder, inventory saver, gauges |
| `GatewayHandler.java` | Routes requests: store-and-forward, proxy, inventory, metrics, health |
| `DiskQueue.java` | The store-and-forward queue, one file per record |
| `Forwarder.java` | One thread draining the queue to the cloud, in order |
| `Credentials.java` | The newest token each device has presented |
| `UpstreamClient.java` | HTTP to the cloud, with a concurrency cap and timeouts |
| `Inventory.java` | Which devices the site has, and when each was last heard from |
| `MetricsRegistry.java`, `GatewayMetrics.java`, `JvmMetrics.java` | Prometheus text format, the gateway's series, JVM series |
| `Log.java` | JSON logs in the same shape as the Go services |

## Two kinds of request

```
device ──▶ GatewayHandler ─┬─ telemetry, alerts ──▶ storeAndForward ──▶ cloud, or DiskQueue ──▶ Forwarder ──▶ cloud
                           └─ everything else ────▶ proxy ───────────▶ cloud
```

**Store-and-forward** (`POST /v1/telemetry`, `POST /v1/alerts`). If the queue is empty, the request goes straight to the cloud and the cloud's answer goes back to the device. If the cloud fails in a way that might pass (5xx, timeout, unreachable, too busy), or if anything is already queued, the request is written to the disk queue and the device gets `202 {"queued": true}`.

The "anything already queued" rule matters twice. It keeps order: a new batch never overtakes older ones. And it protects a recovering cloud: after an outage the cloud sees one paced forwarder, not a hundred devices at once.

**Proxy** (registration, heartbeats, bundle upload). These pass through. Devices already retry them, and bundles have their own spool on the device, so a second queue here would add nothing. If the cloud doesn't answer, the device gets a 503 with `Retry-After`.

Alerts share the telemetry queue: one order, one set of limits, one thing to monitor.

## The disk queue

One file per record, named by a 20-digit sequence number, so a directory listing in name order is the queue. An in-memory index (a skip-list map) mirrors the directory; it is rebuilt from the directory at startup.

Writing a record (`append`):

1. Write the bytes to `<seq>.json.tmp` and flush them to the disk (`force`).
2. Rename to `<seq>.json`. The rename is atomic, so a reader never sees half a record.
3. Flush the directory, so the rename itself survives.

Only then does the device get its 202. Steps 1 and 3 are what make the 202 true through a power cut. Without them the data sits in the operating system's cache for seconds, and a rename can be lost or can point at an empty file. A gateway box at a customer site loses power far more often than a cloud server does.

Leftover `.tmp` files are deleted at startup. A file that can't be parsed is dropped and counted as `corrupt`.

The queue is bounded by record count and by bytes. When full it evicts the oldest records, the same choice the devices make with their own buffers, and counts them. That is data loss and it pages.

Each record stores the device ID, the Authorization header it arrived with, the route to deliver it to, when it arrived, the body, and the device's `traceparent`, so the forwarded request joins the device's trace however long it waited.

## The forwarder

A single thread takes up to 100 records from the head and sends them one at a time.

| Cloud's answer | Forwarder |
|---|---|
| 2xx | Removes the record, moves on |
| 5xx, timeout, unreachable, 408, 429 | Stops, backs off (exponential, with jitter), retries the same record |
| 401, 403 | See below |
| Any other 4xx | Removes the record and counts it as `rejected`: retrying can't succeed, and it would block everything behind it |

One thread is a deliberate trade: delivery in arrival order and a gentle recovery, at the price of drain speed. With a slow uplink the queue drains at one request per round trip.

Each forwarded request carries `X-Gateway-Queued-Ms`, the time the record waited, as a duration measured on the gateway's own clock. A timestamp would be distorted by any difference between the site's clock and the cloud's.

### Credentials that stop working

A 401 or 403 says the credentials are wrong, not the record. Credentials change: the cloud's signing secret is rotated, or a deploy ships the wrong one. If the forwarder dropped records on a 401, a rotation would destroy every backlog in the fleet.

So:

1. `GatewayHandler` remembers the newest Authorization header each device presents (`Credentials`). The forwarder sends each record with its device's newest header.
2. After a rotation, devices get a 401 on their next pass-through call, register again, and start presenting a new token. Their queued records then drain with it.
3. If the newest header is refused, the forwarder tries the record's own, in case something else on the site LAN claimed that device ID with a bad token.
4. If both are refused, the forwarder holds the queue and counts `forward_failures_total{reason="http_401"}`. `GatewayBacklogNotDraining` pages.
5. If nothing has been delivered for `AUTH_REJECT_GRACE` (30 minutes), records are given up on one by one until one is accepted. That bounds how long a device with a permanently bad token can hold up a site.

## Calling the cloud

`UpstreamClient` caps concurrent cloud requests with a semaphore. A caller that can't get a slot within `UPSTREAM_ACQUIRE_TIMEOUT` gets `BUSY`, which is treated like an unavailable cloud. Without the cap, a slow cloud would hold every HTTP worker on a pass-through call.

Ordinary requests have `UPSTREAM_TIMEOUT` (10 seconds). Bundle chunks and bundle completion have `UPSTREAM_BULK_TIMEOUT` (2 minutes): they carry far more bytes, and on a thin uplink a chunk that must cross in 10 seconds never arrives.

Only a fixed set of headers is forwarded. Everything else a device sends stays at the site.

## HTTP workers

Requests are served by a fixed pool with a bounded queue, like a servlet container's connector. When both are full the connection is dropped and counted. A bounded pool fails visibly and early; an unbounded one fails as an out-of-memory error much later.

## Inventory

Every request updates the device's last-seen time. A device is online, stale after `DEVICE_STALE_AFTER`, and offline after `DEVICE_OFFLINE_AFTER`. The inventory is saved to disk periodically, only when it has changed.

## Readiness

`/readyz` checks that the queue directory is writable and nothing else. If readiness followed the cloud, the gateway would be taken out of service exactly when the cloud is down, which is the moment it is needed.

## Restart, upgrade, rollback

- **Restart:** the queue and the inventory are on disk; draining resumes where it stopped. The newest-credentials map is in memory and refills as devices send.
- **Upgrade:** unknown fields in a queue file are ignored, and files from before a field existed get its default.
- **Rollback with a backlog:** drain the queue first. A version from before unknown fields were ignored discards newer files as corrupt.

## Known limits

- When the queue is full, alerts are evicted in the same order as telemetry, although they matter more.
- Alerts wait behind older telemetry in the single queue; after a long outage an alert arrives only once everything before it has drained.
- Device tokens are stored in the queue files in plain text.
- The gateway can't verify a token itself, so while a backlog exists it accepts records from any device ID.
