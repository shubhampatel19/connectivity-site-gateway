package io.github.shubhampatel19.connectivity.gateway;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JVM runtime metrics, read straight from the platform MXBeans. These are the
 * graphs you reach for in a JVM incident: heap used against max (leaks,
 * undersized heaps), time spent in GC (pause storms), threads (pool
 * exhaustion, leaked threads) and direct buffers (off-heap growth).
 */
final class JvmMetrics {
    private JvmMetrics() {
    }

    static void register(MetricsRegistry r) {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        r.gaugeVec("jvm_memory_used_bytes", "Used bytes of a JVM memory area.", "area",
                () -> areas(memory, MemoryUsage::getUsed));
        r.gaugeVec("jvm_memory_committed_bytes", "Committed bytes of a JVM memory area.", "area",
                () -> areas(memory, MemoryUsage::getCommitted));
        r.gaugeVec("jvm_memory_max_bytes", "Max bytes of a JVM memory area (-1 when unbounded).", "area",
                () -> areas(memory, MemoryUsage::getMax));

        List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
        r.gaugeVec("jvm_memory_pool_used_bytes", "Used bytes of a JVM memory pool (for example G1 Old Gen).", "pool", () -> {
            Map<String, Double> m = new LinkedHashMap<>();
            pools.forEach(p -> m.put(p.getName(), (double) p.getUsage().getUsed()));
            return m;
        });

        List<GarbageCollectorMXBean> gcs = ManagementFactory.getGarbageCollectorMXBeans();
        r.register(out -> {
            MetricsRegistry.header(out, "jvm_gc_collection_seconds", "Time spent in a garbage collector, and how many collections it ran.", "summary");
            for (GarbageCollectorMXBean gc : gcs) {
                List<String> label = List.of(gc.getName());
                MetricsRegistry.sample(out, "jvm_gc_collection_seconds_count", List.of("gc"), label, gc.getCollectionCount());
                MetricsRegistry.sample(out, "jvm_gc_collection_seconds_sum", List.of("gc"), label, gc.getCollectionTime() / 1000.0);
            }
        });

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        r.gauge("jvm_threads_current", "Live JVM threads.", threads::getThreadCount);
        r.gauge("jvm_threads_daemon", "Live daemon threads.", threads::getDaemonThreadCount);
        r.gauge("jvm_threads_peak", "Peak live threads since start.", threads::getPeakThreadCount);

        List<BufferPoolMXBean> buffers = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        r.gaugeVec("jvm_buffer_pool_used_bytes", "Bytes used by a buffer pool (direct, mapped).", "pool", () -> {
            Map<String, Double> m = new LinkedHashMap<>();
            buffers.forEach(b -> m.put(b.getName(), (double) b.getMemoryUsed()));
            return m;
        });

        var os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean sun) {
            r.gauge("process_cpu_seconds_total", "CPU time used by the process.", () -> sun.getProcessCpuTime() / 1e9);
        }
        if (os instanceof com.sun.management.UnixOperatingSystemMXBean unix) {
            r.gauge("process_open_fds", "Open file descriptors.", unix::getOpenFileDescriptorCount);
            r.gauge("process_max_fds", "File descriptor limit.", unix::getMaxFileDescriptorCount);
        }
        long start = ManagementFactory.getRuntimeMXBean().getStartTime();
        r.gauge("process_start_time_seconds", "Process start time, seconds since the epoch.", () -> start / 1000.0);
    }

    private static Map<String, Double> areas(MemoryMXBean memory, java.util.function.ToLongFunction<MemoryUsage> f) {
        Map<String, Double> m = new LinkedHashMap<>();
        m.put("heap", (double) f.applyAsLong(memory.getHeapMemoryUsage()));
        m.put("nonheap", (double) f.applyAsLong(memory.getNonHeapMemoryUsage()));
        return m;
    }
}
