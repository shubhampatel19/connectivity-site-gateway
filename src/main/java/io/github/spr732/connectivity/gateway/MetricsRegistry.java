package io.github.spr732.connectivity.gateway;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;

/**
 * A small Prometheus registry writing the text exposition format (0.0.4).
 * It is deliberately small enough to read in one sitting: during a drill you
 * should know exactly where every number on the dashboard comes from.
 */
public final class MetricsRegistry {

    /** Anything that can write its own samples. */
    @FunctionalInterface
    public interface Collector {
        void write(StringBuilder out);
    }

    private final List<Collector> collectors = new CopyOnWriteArrayList<>();

    public Counter counter(String name, String help, String... labelNames) {
        Counter c = new Counter(name, help, labelNames);
        collectors.add(c);
        return c;
    }

    public Histogram histogram(String name, String help, double[] buckets, String... labelNames) {
        Histogram h = new Histogram(name, help, buckets, labelNames);
        collectors.add(h);
        return h;
    }

    /** A gauge read at scrape time. */
    public void gauge(String name, String help, DoubleSupplier value) {
        collectors.add(out -> {
            header(out, name, help, "gauge");
            sample(out, name, List.of(), List.of(), value.getAsDouble());
        });
    }

    /** A gauge with one label, read at scrape time. */
    public void gaugeVec(String name, String help, String labelName, Supplier<Map<String, Double>> values) {
        collectors.add(out -> {
            header(out, name, help, "gauge");
            new TreeMap<>(values.get()).forEach((label, v) -> sample(out, name, List.of(labelName), List.of(label), v));
        });
    }

    public void register(Collector c) {
        collectors.add(c);
    }

    public String scrape() {
        StringBuilder out = new StringBuilder(8192);
        for (Collector c : collectors) {
            c.write(out);
        }
        return out.toString();
    }

    // ---- exposition helpers ----

    static void header(StringBuilder out, String name, String help, String type) {
        out.append("# HELP ").append(name).append(' ').append(help.replace("\\", "\\\\").replace("\n", "\\n")).append('\n');
        out.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    static void sample(StringBuilder out, String name, List<String> labelNames, List<String> labelValues, double value) {
        out.append(name);
        if (!labelNames.isEmpty()) {
            out.append('{');
            for (int i = 0; i < labelNames.size(); i++) {
                if (i > 0) {
                    out.append(',');
                }
                out.append(labelNames.get(i)).append("=\"").append(escape(labelValues.get(i))).append('"');
            }
            out.append('}');
        }
        out.append(' ').append(format(value)).append('\n');
    }

    static String escape(String v) {
        return v.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    static String format(double v) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        if (Double.isInfinite(v)) {
            return v > 0 ? "+Inf" : "-Inf";
        }
        if (v == Math.rint(v) && Math.abs(v) < 1e15) {
            return Long.toString((long) v);
        }
        return Double.toString(v);
    }

    private static List<String> labelKey(String[] names, String[] values) {
        if (names.length != values.length) {
            throw new IllegalArgumentException("expected " + names.length + " label values, got " + values.length);
        }
        return List.of(values);
    }

    // ---- counter ----

    public static final class Counter implements Collector {
        private final String name;
        private final String help;
        private final String[] labelNames;
        private final ConcurrentHashMap<List<String>, DoubleAdder> children = new ConcurrentHashMap<>();

        private Counter(String name, String help, String[] labelNames) {
            this.name = name;
            this.help = help;
            this.labelNames = labelNames;
        }

        public DoubleAdder labels(String... values) {
            return children.computeIfAbsent(labelKey(labelNames, values), k -> new DoubleAdder());
        }

        public void inc() {
            labels().add(1);
        }

        public void inc(double v) {
            labels().add(v);
        }

        public double get(String... values) {
            DoubleAdder a = children.get(List.of(values));
            return a == null ? 0 : a.sum();
        }

        @Override
        public void write(StringBuilder out) {
            header(out, name, help, "counter");
            new TreeMap<>(children.entrySet().stream().collect(
                    java.util.stream.Collectors.toMap(e -> String.join("\u0000", e.getKey()), e -> e)))
                    .values()
                    .forEach(e -> sample(out, name, Arrays.asList(labelNames), e.getKey(), e.getValue().sum()));
        }
    }

    // ---- histogram ----

    public static final class Histogram implements Collector {
        private final String name;
        private final String help;
        private final double[] bounds;
        private final String[] labelNames;
        private final ConcurrentHashMap<List<String>, Child> children = new ConcurrentHashMap<>();

        private Histogram(String name, String help, double[] bounds, String[] labelNames) {
            this.name = name;
            this.help = help;
            this.bounds = bounds.clone();
            Arrays.sort(this.bounds);
            this.labelNames = labelNames;
        }

        public Child labels(String... values) {
            return children.computeIfAbsent(labelKey(labelNames, values), k -> new Child(bounds));
        }

        public void observe(double v) {
            labels().observe(v);
        }

        public long count(String... values) {
            Child c = children.get(List.of(values));
            return c == null ? 0 : c.count.sum();
        }

        @Override
        public void write(StringBuilder out) {
            header(out, name, help, "histogram");
            List<String> bucketLabels = new ArrayList<>(Arrays.asList(labelNames));
            bucketLabels.add("le");
            new TreeMap<>(children.entrySet().stream().collect(
                    java.util.stream.Collectors.toMap(e -> String.join("\u0000", e.getKey()), e -> e)))
                    .values()
                    .forEach(e -> {
                        Child c = e.getValue();
                        long cumulative = 0;
                        for (int i = 0; i <= bounds.length; i++) {
                            cumulative += c.buckets[i].sum();
                            List<String> values = new ArrayList<>(e.getKey());
                            values.add(i < bounds.length ? format(bounds[i]) : "+Inf");
                            sample(out, name + "_bucket", bucketLabels, values, cumulative);
                        }
                        sample(out, name + "_sum", Arrays.asList(labelNames), e.getKey(), c.sum.sum());
                        sample(out, name + "_count", Arrays.asList(labelNames), e.getKey(), c.count.sum());
                    });
        }

        public static final class Child {
            private final double[] bounds;
            private final LongAdder[] buckets;
            private final DoubleAdder sum = new DoubleAdder();
            private final LongAdder count = new LongAdder();

            private Child(double[] bounds) {
                this.bounds = bounds;
                this.buckets = new LongAdder[bounds.length + 1];
                for (int i = 0; i < buckets.length; i++) {
                    buckets[i] = new LongAdder();
                }
            }

            public void observe(double v) {
                int i = 0;
                while (i < bounds.length && v > bounds[i]) {
                    i++;
                }
                buckets[i].increment();
                sum.add(v);
                count.increment();
            }
        }
    }
}
