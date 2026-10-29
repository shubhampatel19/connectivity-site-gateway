package io.github.spr732.connectivity.gateway;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/** A stand-in for the cloud ingest API that records every request it gets. */
final class FakeCloud implements AutoCloseable {
    record Request(String method, String path, Map<String, String> headers, byte[] body, int status) {
        String bodyText() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    final List<Request> requests = new CopyOnWriteArrayList<>();
    volatile int telemetryStatus = 202;
    volatile int otherStatus = 200;
    private final HttpServer server;
    private boolean closed;

    FakeCloud() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            byte[] body = ex.getRequestBody().readAllBytes();
            Map<String, String> h = new HashMap<>();
            ex.getRequestHeaders().forEach((k, v) -> h.put(k.toLowerCase(Locale.ROOT), v.get(0)));
            String path = ex.getRequestURI().getPath();
            int code = path.equals("/v1/telemetry") ? telemetryStatus : otherStatus;
            requests.add(new Request(ex.getRequestMethod(), path, h, body, code));
            byte[] resp = path.equals("/v1/register") && code == 200
                    ? "{\"device_id\":\"dev-1\",\"token\":\"tok\"}".getBytes(StandardCharsets.UTF_8)
                    : "{}".getBytes(StandardCharsets.UTF_8);
            if (code == 503) {
                ex.getResponseHeaders().set("Retry-After", "7");
            }
            ex.getResponseHeaders().set("Content-Type", "application/json");
            if (code == 204) {
                ex.sendResponseHeaders(204, -1);
            } else {
                ex.sendResponseHeaders(code, resp.length);
                ex.getResponseBody().write(resp);
            }
            ex.close();
        });
        server.start();
    }

    URI uri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    List<Request> to(String path) {
        return requests.stream().filter(r -> r.path().equals(path)).toList();
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            server.stop(0);
        }
    }
}
