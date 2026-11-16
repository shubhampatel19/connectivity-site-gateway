package io.github.shubhampatel19.connectivity.gateway;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The newest credentials each device has presented.
 *
 * <p>A queued record carries the Authorization header the device sent with
 * it. If the cloud's signing secret changes while the record waits, that
 * header is no longer valid, and delivering the record with it would be
 * refused. The device, meanwhile, notices the refusal on its next
 * pass-through call, registers again and starts sending a new token. The
 * forwarder uses that newer token for the device's older records too.
 *
 * <p>Kept in memory only: after a restart the forwarder falls back to each
 * record's own header until its device is heard from again.
 */
final class Credentials {
    private final Map<String, String> latest = new ConcurrentHashMap<>();

    void seen(String deviceId, String authorization) {
        if (deviceId != null && authorization != null && !authorization.isBlank()) {
            latest.put(deviceId, authorization);
        }
    }

    /** The newest header seen for the device, or {@code fallback} if none has been. */
    String latest(String deviceId, String fallback) {
        return latest.getOrDefault(deviceId, fallback);
    }
}
