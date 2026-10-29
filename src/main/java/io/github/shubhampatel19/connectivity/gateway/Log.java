package io.github.shubhampatel19.connectivity.gateway;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintStream;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Structured JSON logs on stdout, one object per line, using the same keys as
 * the Go services' slog output (time, level, msg, service), so one log query
 * works across the whole stack.
 */
public final class Log {
    public enum Level { DEBUG, INFO, WARN, ERROR }

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Map<String, Object> fields;
    private final PrintStream out;
    private final Level min;

    private Log(Map<String, Object> fields, PrintStream out, Level min) {
        this.fields = fields;
        this.out = out;
        this.min = min;
    }

    public static Log root(String service) {
        return to(System.out, service, parseLevel(System.getenv("LOG_LEVEL")));
    }

    static Log to(PrintStream out, String service, Level min) {
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("service", service);
        return new Log(f, out, min);
    }

    static Level parseLevel(String s) {
        if (s == null) {
            return Level.INFO;
        }
        try {
            return Level.valueOf(s.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return Level.INFO;
        }
    }

    public Log with(String key, Object value) {
        Map<String, Object> f = new LinkedHashMap<>(fields);
        f.put(key, value);
        return new Log(f, out, min);
    }

    public boolean enabled(Level level) {
        return level.ordinal() >= min.ordinal();
    }

    public void debug(String msg, Object... kv) {
        log(Level.DEBUG, msg, kv);
    }

    public void info(String msg, Object... kv) {
        log(Level.INFO, msg, kv);
    }

    public void warn(String msg, Object... kv) {
        log(Level.WARN, msg, kv);
    }

    public void error(String msg, Object... kv) {
        log(Level.ERROR, msg, kv);
    }

    public void log(Level level, String msg, Object... kv) {
        if (!enabled(level)) {
            return;
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("time", Instant.now().toString());
        m.put("level", level.name());
        m.put("msg", msg);
        m.putAll(fields);
        for (int i = 0; i + 1 < kv.length; i += 2) {
            Object v = kv[i + 1];
            m.put(String.valueOf(kv[i]), v instanceof Throwable || v instanceof java.time.Duration ? v.toString() : v);
        }
        String line;
        try {
            line = JSON.writeValueAsString(m);
        } catch (JsonProcessingException e) {
            line = "{\"level\":\"ERROR\",\"msg\":\"unloggable message\",\"original\":" + JSON.valueToTree(msg) + "}";
        }
        synchronized (out) {
            out.println(line);
        }
    }
}
