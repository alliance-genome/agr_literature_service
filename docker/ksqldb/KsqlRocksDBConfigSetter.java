package org.alliancegenome.ksql;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RateLimiter;
import org.rocksdb.RateLimiterMode;
import org.rocksdb.WriteBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB tuning for ksqlDB state stores (SCRUM-6231).
 *
 * <p>The Debezium -&gt; ksqlDB -&gt; Elasticsearch reindex is throughput-bound at the m5.4xlarge
 * ~594 MB/s instance EBS-bandwidth ceiling: ksqlDB's RocksDB state-store compaction writes dominate
 * the disk. The env-only knobs (cache/heap/commit/topology/zstd-producer/standby) cannot reach
 * RocksDB's on-disk behaviour, and {@code num.stream.threads=2} only stretched the run without
 * shaving the peak (compaction write-amp is driven by data volume, not thread count). The
 * volume-cutting levers require a {@link RocksDBConfigSetter} on the ksqlDB classpath, which this
 * class provides.
 *
 * <p>{@link #setConfig} is invoked once per state store (~32 here). Every lever is tunable via
 * {@code KSQL_KSQL_STREAMS_ROCKSDB_*} env (passed through to Kafka Streams, surfacing here in the
 * {@code configs} map), with safe defaults, so the image need not be rebuilt to retune.
 */
public class KsqlRocksDBConfigSetter implements RocksDBConfigSetter {

    private static final Logger LOG = LoggerFactory.getLogger(KsqlRocksDBConfigSetter.class);

    // Log the resolved config once (setConfig runs per state store; we only want one summary line).
    private static volatile boolean loggedConfig = false;

    static final String COMPRESSION_ENABLED = "rocksdb.compression.enabled";
    static final String COMPACTION_UNIVERSAL = "rocksdb.compaction.universal";
    static final String RATELIMIT_ENABLED = "rocksdb.ratelimit.enabled";
    static final String RATELIMIT_DISK_MAX = "rocksdb.ratelimit.disk.max.bytes.per.sec";
    static final String RATELIMIT_FRACTION = "rocksdb.ratelimit.fraction";
    static final String MEMORY_BOUNDED = "rocksdb.memory.bounded";
    static final String MEMORY_TOTAL_OFFHEAP_BYTES = "rocksdb.memory.total.offheap.bytes";
    static final String MEMORY_OFFHEAP_FRACTION = "rocksdb.memory.offheap.fraction";
    static final String MEMORY_WRITE_BUFFER_FRACTION = "rocksdb.memory.write.buffer.fraction";
    static final String WRITE_BUFFER_SIZE_BYTES = "rocksdb.write.buffer.size.bytes";
    static final String MAX_WRITE_BUFFER_NUMBER = "rocksdb.max.write.buffer.number";
    static final String MEMORY_REPORT_ENABLED = "rocksdb.memory.report.enabled";

    // 594 MiB/s == the m5.4xlarge instance EBS-bandwidth ceiling (NOT the gp3 volume cap, which is
    // also 594; the instance baseline is the real wall). Override per-environment for other instances.
    private static final long DEFAULT_DISK_MAX_BYTES_PER_SEC = 594L * 1024 * 1024;
    private static final double DEFAULT_RATELIMIT_FRACTION = 0.8;
    private static final long RATELIMIT_REFILL_PERIOD_MICROS = 100_000L; // 100 ms (RocksDB default)
    private static final int RATELIMIT_FAIRNESS = 10;                    // RocksDB default

    // ONE shared limiter across all stores. A per-store limiter would give an aggregate cap of
    // N * rate and protect nothing -- the whole point is a single instance-wide background-I/O cap.
    private static volatile RateLimiter sharedRateLimiter;

    // --- SCRUM-6318: bound RocksDB OFF-HEAP memory (block caches + memtables) across all stores ---
    // Unbounded, ~32 queries' state stores grew several GB past the JVM heap during the full
    // reindex and historically triggered the kernel OOM killer on swapless boxes. One shared
    // LRUCache with the memtable budget charged INTO it (WriteBufferManager) gives a single
    // instance-wide cap, auto-sized from the RAM actually visible to the container.
    // 0.10 (was 0.15): the SCRUM-6318 swapless pilot showed off-heap plateaued at ~12.7 GiB on a
    // 31 GiB box -- the block cache is only part of it; the dominant term is per-store memtables
    // summed across the ~115 RocksDB instances the FK-join substores create. Shrinking the shared
    // cache AND the per-store memtable floor (below) both matter.
    private static final double DEFAULT_OFFHEAP_FRACTION = 0.10;
    private static final double DEFAULT_WRITE_BUFFER_FRACTION = 0.5;
    private static final long MIN_OFFHEAP_BYTES = 512L * 1024 * 1024;
    private static final long FALLBACK_TOTAL_RAM_BYTES = 8L * 1024 * 1024 * 1024;

    // Per-store memtable floor. With ~115 RocksDB instances, aggregate memtable memory is
    // instances * write_buffer_size * max_write_buffer_number, and it is additive to the block
    // cache whenever the WriteBufferManager's charge-into-cache is not honoured through the Kafka
    // Streams Options adapter. Kafka Streams' own defaults (16 MiB * 3 = 48 MiB/store) put that
    // floor at ~5.5 GiB; 8 MiB * 2 = 16 MiB/store cuts it to ~1.8 GiB. Verified in a rocksdbjni
    // harness: 455.8 -> 167.8 MiB retained memtable for the same write set (2.7x). Both are
    // serialized to the store's OPTIONS file, so the applied value is verifiable on the box.
    private static final long DEFAULT_WRITE_BUFFER_SIZE_BYTES = 8L * 1024 * 1024;
    private static final int DEFAULT_MAX_WRITE_BUFFER_NUMBER = 2;
    // Share of the cache reserved as high-priority space for index/filter blocks, so bulk data
    // scans cannot evict them (matches ksqlDB's bounded-memory reference implementation).
    private static final double INDEX_FILTER_BLOCK_RATIO = 0.1;

    private static volatile Cache sharedCache;
    private static volatile WriteBufferManager sharedWriteBufferManager;

    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
        final boolean compression = getBoolean(configs, COMPRESSION_ENABLED, true);
        final boolean universal = getBoolean(configs, COMPACTION_UNIVERSAL, true);
        final boolean ratelimit = getBoolean(configs, RATELIMIT_ENABLED, true);

        if (!loggedConfig) {
            loggedConfig = true;
            // One summary line at INFO so the applied config is visible in the ksqlDB log. Also echo
            // the raw values found for the toggles, to catch a config that silently disables a lever.
            LOG.info("KsqlRocksDBConfigSetter resolved: compression={} (raw={}), universal_compaction={} (raw={}), "
                    + "ratelimit={} (raw={})", compression, configs.get(COMPRESSION_ENABLED),
                    universal, configs.get(COMPACTION_UNIVERSAL), ratelimit, configs.get(RATELIMIT_ENABLED));
        }

        if (compression) {
            // LZ4 on the hot upper levels (cheap CPU), ZSTD on the bottommost level (best ratio,
            // where most of the bytes live) -> fewer bytes read+written by every compaction.
            options.setCompressionType(CompressionType.LZ4_COMPRESSION);
            options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
        }
        if (universal) {
            // Universal trades transient disk SPACE (abundant: ~139 GB state on a 1500 GiB volume)
            // for far lower write-amplification than leveled -> fewer, cheaper compactions.
            // Requires a fresh store; safe here because restart-debezium-* wipes ksqlDB state.
            options.setCompactionStyle(CompactionStyle.UNIVERSAL);
        }
        if (ratelimit) {
            options.setRateLimiter(sharedRateLimiter(configs));
        }
        final boolean bounded = getBoolean(configs, MEMORY_BOUNDED, true);
        if (bounded) {
            initBoundedMemory(configs);
            // Per-store memtable floor (lever 1). Applied to EVERY store, so the aggregate across
            // all ~115 instances is bounded even if the shared WriteBufferManager charge-into-cache
            // is not honoured by the Kafka Streams adapter.
            final long writeBufferSize = getLong(configs, WRITE_BUFFER_SIZE_BYTES, DEFAULT_WRITE_BUFFER_SIZE_BYTES);
            final int maxWriteBufferNumber =
                    (int) getLong(configs, MAX_WRITE_BUFFER_NUMBER, DEFAULT_MAX_WRITE_BUFFER_NUMBER);
            options.setWriteBufferSize(writeBufferSize);
            options.setMaxWriteBufferNumber(maxWriteBufferNumber);
            final Object tf = options.tableFormatConfig();
            if (tf instanceof BlockBasedTableConfig) {
                final BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) tf;
                tableConfig.setBlockCache(sharedCache);
                // Count index/filter blocks against the same budget and keep them in the cache's
                // high-priority pool, so the cap covers ALL block memory, not just data blocks.
                tableConfig.setCacheIndexAndFilterBlocks(true);
                tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
                tableConfig.setPinTopLevelIndexAndFilter(true);
                options.setTableFormatConfig(tableConfig);
            } else {
                LOG.warn("Store {} table format is {}; block cache left unbounded for this store",
                        storeName, tf == null ? "null" : tf.getClass().getName());
            }
            options.setWriteBufferManager(sharedWriteBufferManager);
        }
        LOG.debug("KsqlRocksDBConfigSetter applied to store {} (compression={}, universal={}, ratelimit={}, "
                + "boundedMemory={})", storeName, compression, universal, ratelimit, bounded);
    }

    private static void initBoundedMemory(final Map<String, Object> configs) {
        if (sharedCache != null) {
            return;
        }
        synchronized (KsqlRocksDBConfigSetter.class) {
            if (sharedCache != null) {
                return;
            }
            long total = getLong(configs, MEMORY_TOTAL_OFFHEAP_BYTES, 0L);
            final String sizedFrom;
            if (total <= 0) {
                final long ram = detectTotalMemoryBytes();
                final double fraction = getDouble(configs, MEMORY_OFFHEAP_FRACTION, DEFAULT_OFFHEAP_FRACTION);
                total = (long) (ram * fraction);
                sizedFrom = "auto: detectedRam=" + ram + " * fraction=" + fraction;
            } else {
                sizedFrom = "explicit " + MEMORY_TOTAL_OFFHEAP_BYTES;
            }
            if (total < MIN_OFFHEAP_BYTES) {
                total = MIN_OFFHEAP_BYTES;
            }
            final double wbFraction =
                    getDouble(configs, MEMORY_WRITE_BUFFER_FRACTION, DEFAULT_WRITE_BUFFER_FRACTION);
            final long writeBufferBytes = (long) (total * wbFraction);
            final Cache cache = new LRUCache(total, -1, false, INDEX_FILTER_BLOCK_RATIO);
            // Charging memtables INTO the block cache makes `total` the single budget for all
            // RocksDB off-heap across every store in the JVM.
            sharedWriteBufferManager = new WriteBufferManager(writeBufferBytes, cache);
            sharedCache = cache;
            LOG.info("KsqlRocksDBConfigSetter bounded RocksDB off-heap: total=" + total + " B (" + sizedFrom
                    + "), writeBufferBudget=" + writeBufferBytes + " B, per-store memtable floor="
                    + getLong(configs, WRITE_BUFFER_SIZE_BYTES, DEFAULT_WRITE_BUFFER_SIZE_BYTES) + " B x "
                    + (int) getLong(configs, MAX_WRITE_BUFFER_NUMBER, DEFAULT_MAX_WRITE_BUFFER_NUMBER));
            if (getBoolean(configs, MEMORY_REPORT_ENABLED, true)) {
                startMemoryReporter();
            }
        }
    }

    /**
     * SCRUM-6318 diagnostic: the ~12 GiB off-heap growth on 2.7 is not memtables (hard-capped by
     * write_buffer_size x max_write_buffer_number, verified in each store's OPTIONS file). This
     * daemon reports, from inside the JVM every 30 s, the actual usage of each off-heap bucket so
     * the growing one can be identified without JMX/NMT:
     *   - sharedBlockCache usage + pinned  (if this stays near 0, ksqlDB is NOT using our shared
     *     cache and each of the ~115 stores keeps its own default cache instead)
     *   - JVM non-heap (metaspace/code cache) and direct byte buffers (Netty/Kafka client)
     */
    private static void startMemoryReporter() {
        final Thread t = new Thread(() -> {
            final java.lang.management.MemoryMXBean mem =
                    java.lang.management.ManagementFactory.getMemoryMXBean();
            final java.util.List<java.lang.management.BufferPoolMXBean> pools =
                    java.lang.management.ManagementFactory.getPlatformMXBeans(
                            java.lang.management.BufferPoolMXBean.class);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(30_000L);
                    long cacheUsage = -1;
                    long cachePinned = -1;
                    if (sharedCache != null) {
                        cacheUsage = sharedCache.getUsage();
                        cachePinned = sharedCache.getPinnedUsage();
                    }
                    long direct = 0;
                    for (final java.lang.management.BufferPoolMXBean p : pools) {
                        if ("direct".equals(p.getName())) {
                            direct = p.getMemoryUsed();
                        }
                    }
                    LOG.info("MEMREPORT sharedCacheUsage={} sharedCachePinned={} heapUsed={} "
                            + "nonHeapUsed={} directBuffers={} (bytes)",
                            cacheUsage, cachePinned, mem.getHeapMemoryUsage().getUsed(),
                            mem.getNonHeapMemoryUsage().getUsed(), direct);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (final Exception e) {
                    LOG.warn("MEMREPORT error: {}", e.toString());
                }
            }
        }, "ksql-rocksdb-memreport");
        t.setDaemon(true);
        t.start();
        LOG.info("KsqlRocksDBConfigSetter MEMREPORT diagnostic thread started (30s interval)");
    }

    /**
     * RAM visible to this container: host MemTotal, capped by the cgroup memory limit when one is
     * set (cgroup v2 then v1). Falls back to a conservative 8 GiB if neither is readable.
     */
    private static long detectTotalMemoryBytes() {
        long total = FALLBACK_TOTAL_RAM_BYTES;
        try {
            for (final String line : Files.readAllLines(Path.of("/proc/meminfo"))) {
                if (line.startsWith("MemTotal:")) {
                    total = Long.parseLong(line.replaceAll("[^0-9]", "")) * 1024L;
                    break;
                }
            }
        } catch (final Exception e) {
            LOG.warn("Could not read /proc/meminfo ({}); assuming {} bytes", e.toString(), total);
        }
        for (final String path : new String[]{"/sys/fs/cgroup/memory.max",
                                              "/sys/fs/cgroup/memory/memory.limit_in_bytes"}) {
            try {
                final String raw = Files.readString(Path.of(path)).trim();
                if (!raw.isEmpty() && !"max".equals(raw)) {
                    final long limit = Long.parseLong(raw);
                    if (limit > 0 && limit < total) {
                        total = limit;
                    }
                }
                break;
            } catch (final Exception ignored) {
                // cgroup v2 file absent -> try the v1 path; neither readable -> keep MemTotal.
            }
        }
        return total;
    }

    private static RateLimiter sharedRateLimiter(final Map<String, Object> configs) {
        RateLimiter limiter = sharedRateLimiter;
        if (limiter == null) {
            synchronized (KsqlRocksDBConfigSetter.class) {
                limiter = sharedRateLimiter;
                if (limiter == null) {
                    final long diskMax = getLong(configs, RATELIMIT_DISK_MAX, DEFAULT_DISK_MAX_BYTES_PER_SEC);
                    final double fraction = getDouble(configs, RATELIMIT_FRACTION, DEFAULT_RATELIMIT_FRACTION);
                    final long cap = (long) (diskMax * fraction);
                    // Auto-tuned: the actual throttle scales with compaction demand UP TO the cap,
                    // leaving headroom (default ~20%) for other ops on the shared disk.
                    limiter = new RateLimiter(cap, RATELIMIT_REFILL_PERIOD_MICROS, RATELIMIT_FAIRNESS,
                            RateLimiterMode.WRITES_ONLY, true);
                    sharedRateLimiter = limiter;
                    LOG.info("KsqlRocksDBConfigSetter shared RateLimiter cap=" + cap + " B/s (diskMax="
                            + diskMax + " * fraction=" + fraction + ", auto-tuned, writes-only)");
                }
            }
        }
        return limiter;
    }

    @Override
    public void close(final String storeName, final Options options) {
        // The shared RateLimiter, LRUCache and WriteBufferManager are intentionally NOT disposed
        // here: they are reused across all stores and live for the JVM lifetime (native objects,
        // freed on process exit). Disposing them when a single store closes would corrupt the
        // instances still in use by the other stores.
    }

    private static boolean getBoolean(final Map<String, Object> configs, final String key, final boolean def) {
        final Object v = configs.get(key);
        return v == null ? def : Boolean.parseBoolean(v.toString().trim());
    }

    private static long getLong(final Map<String, Object> configs, final String key, final long def) {
        final Object v = configs.get(key);
        if (v == null) {
            return def;
        }
        try {
            return Long.parseLong(v.toString().trim());
        } catch (final NumberFormatException e) {
            LOG.warn("Bad long for {}='{}', using default {}", key, v, def);
            return def;
        }
    }

    private static double getDouble(final Map<String, Object> configs, final String key, final double def) {
        final Object v = configs.get(key);
        if (v == null) {
            return def;
        }
        try {
            return Double.parseDouble(v.toString().trim());
        } catch (final NumberFormatException e) {
            LOG.warn("Bad double for {}='{}', using default {}", key, v, def);
            return def;
        }
    }
}
