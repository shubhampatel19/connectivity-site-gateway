package io.github.spr732.connectivity.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UnitTests {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Nested
    class ConfigTests {
        @Test
        void defaultsAreValid() {
            Config c = Config.fromEnv(Map.of());
            assertEquals(8090, c.port());
            assertEquals(Duration.ofSeconds(10), c.upstreamTimeout());
        }

        @Test
        void goStyleDurations() {
            assertEquals(Duration.ofMillis(500), Config.parseDuration("500ms"));
            assertEquals(Duration.ofSeconds(90), Config.parseDuration("1m30s"));
            assertEquals(Duration.ofMillis(1500), Config.parseDuration("1.5s"));
            assertEquals(Duration.ZERO, Config.parseDuration("0"));
            assertThrows(IllegalArgumentException.class, () -> Config.parseDuration("5"));
            assertThrows(IllegalArgumentException.class, () -> Config.parseDuration("1x"));
            assertThrows(IllegalArgumentException.class, () -> Config.parseDuration("s"));
        }

        @Test
        void everyBadSettingIsReportedAtOnce() {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> Config.fromEnv(Map.of("PORT", "abc", "UPSTREAM_URL", "not a url", "HTTP_THREADS", "0")));
            assertTrue(e.getMessage().contains("PORT"));
            assertTrue(e.getMessage().contains("UPSTREAM_URL"));
            assertTrue(e.getMessage().contains("HTTP_THREADS"));
        }
    }

    @Nested
    class MetricsTests {
        @Test
        void counterRendersSortedLabelledSamples() {
            MetricsRegistry r = new MetricsRegistry();
            MetricsRegistry.Counter c = r.counter("reqs_total", "Requests.", "route", "code");
            c.labels("telemetry", "202").add(3);
            c.labels("register", "200").add(1);
            String out = r.scrape();
            assertTrue(out.contains("# TYPE reqs_total counter"));
            assertTrue(out.indexOf("route=\"register\"") < out.indexOf("route=\"telemetry\""), "children sorted");
            assertTrue(out.contains("reqs_total{route=\"telemetry\",code=\"202\"} 3\n"));
        }

        @Test
        void histogramBucketsAreCumulative() {
            MetricsRegistry r = new MetricsRegistry();
            MetricsRegistry.Histogram h = r.histogram("lat_seconds", "Latency.", new double[] {0.1, 1});
            h.observe(0.05);
            h.observe(0.5);
            h.observe(5);
            String out = r.scrape();
            assertTrue(out.contains("lat_seconds_bucket{le=\"0.1\"} 1\n"));
            assertTrue(out.contains("lat_seconds_bucket{le=\"1\"} 2\n"));
            assertTrue(out.contains("lat_seconds_bucket{le=\"+Inf\"} 3\n"));
            assertTrue(out.contains("lat_seconds_count 3\n"));
            assertTrue(out.contains("lat_seconds_sum 5.55\n"));
        }

        @Test
        void labelValuesAreEscaped() {
            assertEquals("a\\\"b\\\\c\\nd", MetricsRegistry.escape("a\"b\\c\nd"));
            assertEquals("42", MetricsRegistry.format(42.0));
            assertEquals("+Inf", MetricsRegistry.format(Double.POSITIVE_INFINITY));
        }

        @Test
        void wrongLabelCountFailsLoudly() {
            MetricsRegistry.Counter c = new MetricsRegistry().counter("x_total", "X.", "a");
            assertThrows(IllegalArgumentException.class, () -> c.labels("1", "2"));
        }
    }

    @Nested
    class DiskQueueTests {
        @TempDir
        Path dir;

        private DiskQueue.Item item(String batch) {
            return new DiskQueue.Item("dev-1", "Bearer t", batch, 0, "{\"batch_id\":\"" + batch + "\"}");
        }

        @Test
        void fifoAndDurableAcrossReopen() throws Exception {
            DiskQueue q = new DiskQueue(dir, 100, 1 << 20, JSON);
            q.append(item("b1"));
            q.append(item("b2"));
            q.remove(q.peek(1).get(0));
            q.append(item("b3"));

            DiskQueue reopened = new DiskQueue(dir, 100, 1 << 20, JSON);
            List<DiskQueue.Entry> entries = reopened.peek(10);
            assertEquals(2, entries.size());
            assertEquals("b2", reopened.read(entries.get(0)).batchId());
            assertEquals("b3", reopened.read(entries.get(1)).batchId());
            reopened.append(item("b4"));
            assertEquals("b4", reopened.read(reopened.peek(10).get(2)).batchId(), "sequence continues after reopen");
            assertEquals(Files.size(entries.get(0).path()) + Files.size(entries.get(1).path())
                    + Files.size(reopened.peek(10).get(2).path()), reopened.bytes());
        }

        @Test
        void evictsOldestWhenOverRecordLimit() throws Exception {
            DiskQueue q = new DiskQueue(dir, 2, 1 << 20, JSON);
            q.append(item("b1"));
            q.append(item("b2"));
            assertEquals(1, q.append(item("b3")));
            List<DiskQueue.Entry> left = q.peek(10);
            assertEquals("b2", q.read(left.get(0)).batchId());
            assertEquals("b3", q.read(left.get(1)).batchId());
        }

        @Test
        void evictsOldestWhenOverByteLimit() throws Exception {
            long one = JSON.writeValueAsBytes(item("b1")).length;
            DiskQueue q = new DiskQueue(dir, 100, one * 2, JSON);
            q.append(item("b1"));
            q.append(item("b2"));
            assertEquals(1, q.append(item("b3")));
            assertTrue(q.bytes() <= one * 2);
        }

        @Test
        void halfWrittenFilesAreCleanedUp() throws Exception {
            Files.writeString(dir.resolve("00000000000000000007.json.tmp"), "{");
            DiskQueue q = new DiskQueue(dir, 100, 1 << 20, JSON);
            assertTrue(q.isEmpty());
            assertFalse(Files.exists(dir.resolve("00000000000000000007.json.tmp")));
        }

        @Test
        void readingAnEvictedEntryReturnsNull() throws Exception {
            DiskQueue q = new DiskQueue(dir, 100, 1 << 20, JSON);
            q.append(item("b1"));
            DiskQueue.Entry e = q.peek(1).get(0);
            q.remove(e);
            assertNull(q.read(e));
        }
    }

    @Nested
    class InventoryTests {
        @TempDir
        Path dir;

        @Test
        void statusAgesAndRegistrationKeepsFirstSeen() {
            MutableClock clock = new MutableClock(Instant.parse("2026-10-29T12:00:00Z"));
            Inventory inv = new Inventory(clock, Duration.ofMinutes(1), Duration.ofMinutes(5));
            inv.seen("dev-1");
            clock.advance(Duration.ofSeconds(30));
            inv.registered("dev-1", "edge-server-e1", "1.4.2", "site-b");
            Inventory.View v = inv.list().get(0);
            assertEquals("2026-10-29T12:00:00Z", v.firstSeen());
            assertEquals("edge-server-e1", v.model());
            assertEquals("online", v.status());

            clock.advance(Duration.ofMinutes(2));
            assertEquals("stale", inv.list().get(0).status());
            clock.advance(Duration.ofMinutes(5));
            assertEquals(1.0, inv.countsByStatus().get("offline"));
            assertEquals(0.0, inv.countsByStatus().get("online"));
        }

        @Test
        void savesOnlyWhenChangedAndReloads() throws Exception {
            MutableClock clock = new MutableClock(Instant.parse("2026-10-29T12:00:00Z"));
            Path file = dir.resolve("inventory.json");
            Inventory inv = new Inventory(clock, Duration.ofMinutes(1), Duration.ofMinutes(5));
            assertFalse(inv.saveIfDirty(file, JSON), "nothing to save yet");
            inv.registered("dev-1", "m", "f", "s");
            assertTrue(inv.saveIfDirty(file, JSON));
            assertFalse(inv.saveIfDirty(file, JSON), "unchanged since last save");

            Inventory reloaded = new Inventory(clock, Duration.ofMinutes(1), Duration.ofMinutes(5));
            reloaded.load(file, JSON);
            assertEquals("dev-1", reloaded.list().get(0).deviceId());
        }
    }
}
