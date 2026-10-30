package io.github.shubhampatel19.connectivity.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The store-and-forward queue: one file per record (a telemetry batch or a
 * device alert), named by a
 * sequence number so a directory listing is the queue order. Files are
 * written to a temp name and renamed, so a crash never leaves a half-written
 * record. The queue is bounded by record count and bytes; when full, the
 * oldest records are evicted, the same trade-off the devices make.
 */
public final class DiskQueue {

    /**
     * One queued request, as received from the device. {@code batchId} holds
     * the record's own ID: the batch ID for telemetry, the alert ID for an
     * alert. {@code path} is the cloud route to deliver it to; records written
     * before alerts existed have none and are telemetry. {@code traceparent}
     * is the device's W3C trace context, kept so the forwarded request joins
     * the device's trace however long the record waits; records written
     * before tracing have none.
     */
    public record Item(String deviceId, String authorization, String batchId, long receivedAt, String body, String path,
                       String traceparent) {
        public static final String TELEMETRY = "/v1/telemetry";
        public static final String ALERTS = "/v1/alerts";

        public Item(String deviceId, String authorization, String batchId, long receivedAt, String body) {
            this(deviceId, authorization, batchId, receivedAt, body, TELEMETRY, null);
        }

        public Item(String deviceId, String authorization, String batchId, long receivedAt, String body, String path) {
            this(deviceId, authorization, batchId, receivedAt, body, path, null);
        }

        /** Where to deliver it; queue files from before the field existed are telemetry. */
        public String target() {
            return path == null ? TELEMETRY : path;
        }

        /** The metrics label for this record's kind. */
        public String kind() {
            return ALERTS.equals(target()) ? "alerts" : "telemetry";
        }
    }

    /** A queue slot. */
    public record Entry(long seq, Path path, long size) {
    }

    private final Path dir;
    private final long maxRecords;
    private final long maxBytes;
    private final ObjectMapper json;
    private final ConcurrentSkipListMap<Long, Entry> index = new ConcurrentSkipListMap<>();
    private final AtomicLong bytes = new AtomicLong();
    private long nextSeq;

    public DiskQueue(Path dir, long maxRecords, long maxBytes, ObjectMapper json) throws IOException {
        this.dir = dir;
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
        this.json = json;
        Files.createDirectories(dir);
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
            for (Path p : files) {
                String name = p.getFileName().toString();
                if (name.endsWith(".tmp")) {
                    Files.deleteIfExists(p);
                    continue;
                }
                if (!name.endsWith(".json")) {
                    continue;
                }
                try {
                    long seq = Long.parseLong(name.substring(0, name.length() - 5));
                    long size = Files.size(p);
                    index.put(seq, new Entry(seq, p, size));
                    bytes.addAndGet(size);
                } catch (NumberFormatException e) {
                    Files.deleteIfExists(p);
                }
            }
        }
        nextSeq = index.isEmpty() ? 1 : index.lastKey() + 1;
    }

    /** Appends an item and returns how many old records were evicted to make room. */
    public synchronized int append(Item item) throws IOException {
        long seq = nextSeq++;
        byte[] data = json.writeValueAsBytes(item);
        Path target = dir.resolve(String.format("%020d.json", seq));
        Path tmp = dir.resolve(target.getFileName() + ".tmp");
        Files.write(tmp, data);
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        index.put(seq, new Entry(seq, target, data.length));
        bytes.addAndGet(data.length);

        int evicted = 0;
        while (index.size() > 1 && (index.size() > maxRecords || bytes.get() > maxBytes)) {
            Map.Entry<Long, Entry> oldest = index.firstEntry();
            if (oldest == null) {
                break;
            }
            remove(oldest.getValue());
            evicted++;
        }
        return evicted;
    }

    /** The oldest n entries, without removing them. */
    public List<Entry> peek(int n) {
        List<Entry> out = new ArrayList<>(Math.min(n, index.size()));
        for (Entry e : index.values()) {
            if (out.size() == n) {
                break;
            }
            out.add(e);
        }
        return out;
    }

    /** Reads an entry, or returns null if it was evicted in the meantime. */
    public Item read(Entry e) throws IOException {
        try {
            return json.readValue(Files.readAllBytes(e.path()), Item.class);
        } catch (NoSuchFileException gone) {
            return null;
        }
    }

    public void remove(Entry e) {
        if (index.remove(e.seq()) != null) {
            bytes.addAndGet(-e.size());
            try {
                Files.deleteIfExists(e.path());
            } catch (IOException ignored) {
                // A leftover file is harmless; it is skipped once out of the index.
            }
        }
    }

    public int size() {
        return index.size();
    }

    public long bytes() {
        return bytes.get();
    }

    public boolean isEmpty() {
        return index.isEmpty();
    }

    public Path dir() {
        return dir;
    }
}
