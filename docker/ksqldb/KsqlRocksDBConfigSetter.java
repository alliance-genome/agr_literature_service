package org.alliancegenome.ksql;

import java.util.Map;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RateLimiter;
import org.rocksdb.RateLimiterMode;
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

    // 594 MiB/s == the m5.4xlarge instance EBS-bandwidth ceiling (NOT the gp3 volume cap, which is
    // also 594; the instance baseline is the real wall). Override per-environment for other instances.
    private static final long DEFAULT_DISK_MAX_BYTES_PER_SEC = 594L * 1024 * 1024;
    private static final double DEFAULT_RATELIMIT_FRACTION = 0.8;
    private static final long RATELIMIT_REFILL_PERIOD_MICROS = 100_000L; // 100 ms (RocksDB default)
    private static final int RATELIMIT_FAIRNESS = 10;                    // RocksDB default

    // ONE shared limiter across all stores. A per-store limiter would give an aggregate cap of
    // N * rate and protect nothing -- the whole point is a single instance-wide background-I/O cap.
    private static volatile RateLimiter sharedRateLimiter;

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
        LOG.debug("KsqlRocksDBConfigSetter applied to store {} (compression={}, universal={}, ratelimit={})",
                storeName, compression, universal, ratelimit);
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
        // The shared RateLimiter is intentionally NOT disposed here: it is reused across all stores
        // and lives for the JVM lifetime (one native object, freed on process exit). Disposing it
        // when a single store closes would corrupt the limiter still in use by the other stores.
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
