package io.github.spr732.connectivity.gateway;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Every device this gateway has seen, with when it was last heard from. A
 * device is online while it keeps talking, stale after a short silence and
 * offline after a long one. Persisted to disk so a gateway restart doesn't
 * forget the site.
 */
public final class Inventory {
    public enum Status { ONLINE, STALE, OFFLINE }

    /** Stored form. Times are epoch milliseconds. */
    public record Device(String deviceId, String model, String firmware, String siteId, long firstSeen, long lastSeen) {
    }

    /** API form. */
    public record View(String deviceId, String model, String firmware, String siteId,
                       String firstSeen, String lastSeen, String status) {
    }

    private final ConcurrentHashMap<String, Device> devices = new ConcurrentHashMap<>();
    private final AtomicBoolean dirty = new AtomicBoolean();
    private final Clock clock;
    private final Duration staleAfter;
    private final Duration offlineAfter;

    public Inventory(Clock clock, Duration staleAfter, Duration offlineAfter) {
        this.clock = clock;
        this.staleAfter = staleAfter;
        this.offlineAfter = offlineAfter;
    }

    /** Records a successful registration, which carries the device's details. */
    public void registered(String id, String model, String firmware, String siteId) {
        long now = clock.millis();
        devices.compute(id, (k, d) -> new Device(id, model, firmware, siteId, d == null ? now : d.firstSeen(), now));
        dirty.set(true);
    }

    /** Records any other request from the device. */
    public void seen(String id) {
        long now = clock.millis();
        devices.compute(id, (k, d) -> d == null
                ? new Device(id, null, null, null, now, now)
                : new Device(id, d.model(), d.firmware(), d.siteId(), d.firstSeen(), now));
        dirty.set(true);
    }

    public Status status(Device d) {
        long silent = clock.millis() - d.lastSeen();
        if (silent < staleAfter.toMillis()) {
            return Status.ONLINE;
        }
        return silent < offlineAfter.toMillis() ? Status.STALE : Status.OFFLINE;
    }

    public List<View> list() {
        List<View> out = new ArrayList<>();
        devices.values().stream().sorted(Comparator.comparing(Device::deviceId)).forEach(d -> out.add(new View(
                d.deviceId(), d.model(), d.firmware(), d.siteId(),
                Instant.ofEpochMilli(d.firstSeen()).toString(), Instant.ofEpochMilli(d.lastSeen()).toString(),
                status(d).name().toLowerCase(Locale.ROOT))));
        return out;
    }

    /** Device counts per status, including zeros, for the gauge. */
    public Map<String, Double> countsByStatus() {
        Map<String, Double> m = new LinkedHashMap<>();
        for (Status s : Status.values()) {
            m.put(s.name().toLowerCase(Locale.ROOT), 0.0);
        }
        devices.values().forEach(d -> m.merge(status(d).name().toLowerCase(Locale.ROOT), 1.0, Double::sum));
        return m;
    }

    public int size() {
        return devices.size();
    }

    public void load(Path file, ObjectMapper json) throws IOException {
        if (!Files.exists(file)) {
            return;
        }
        List<Device> saved = json.readValue(file.toFile(), new TypeReference<List<Device>>() { });
        saved.forEach(d -> devices.put(d.deviceId(), d));
    }

    /** Writes the inventory if it changed since the last save. Atomic: a crash never leaves half a file. */
    public boolean saveIfDirty(Path file, ObjectMapper json) throws IOException {
        if (!dirty.getAndSet(false)) {
            return false;
        }
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            json.writeValue(tmp.toFile(), new ArrayList<>(devices.values()));
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            return true;
        } catch (IOException e) {
            dirty.set(true);
            throw e;
        }
    }
}
