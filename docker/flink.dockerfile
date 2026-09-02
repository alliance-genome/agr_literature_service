# SCRUM-6336: Flink image with the SQL connector jars baked onto the classpath.
#
# Version is parameterised so the 2.1+ MULTI-JOIN operator (FLIP-516) can be evaluated without
# disturbing the known-good 2.0.2 build. Defaults reproduce the validated 2.0.2 image exactly.
#
# WHY 2.1+ MATTERS HERE: this query is 1 INNER + 12 LEFT joins, every one of them on reference_id.
# Flink executes that as a chain of BINARY joins, and each link stores its intermediate result in
# state forever -- so the reference row is materialised ~13 times, once per link, getting wider each
# time. That is a known Flink limitation, fixed in 2.1 by FLIP-516's StreamingMultiJoinOperator,
# which stores only the input records ("zero intermediate state"). It requires multiple INNER/LEFT
# joins sharing at least one common join key -- exactly this query's shape -- and is opt-in via
#   SET 'table.optimizer.multi-join.enabled' = 'true';
# Upstream reports 3x-1000x smaller state.
#
# MEASURED 2026-09-02 AND IT DOES NOT HELP THIS QUERY -- do not reach for it again without new
# evidence. On 2.2.1 the flag engaged cleanly (0 binary Join[] vertices, 6 MultiJoin vertices, 31
# vertices vs 44) and the 2.0 ES connector loaded fine on the 2.2 runtime. But throughput COLLAPSED:
# ~10 docs/s against run 4's ~30 docs/s on the identical binary-join config, projecting ~36h vs ~12h,
# with IOPS still pinned at the 3000 cap, heap only 73-82% and CPU load 0.42 -- so it was neither
# heap- nor CPU-bound, just doing far more I/O per output row (~458 IOPS/doc vs ~100).
# WHY: the multi-join removes STORED intermediate state but must then look up matches across all 13
# inputs on demand, where the binary chain had them pre-joined at one lookup per link. It trades
# storage for lookups, and our bottleneck is cache-miss READS -- so less state cost us more IOPS.
# Note also the optimizer produced SIX MultiJoin operators, not one, so intermediate state persists
# between them and the FLIP is only partially applied to a 13-way LEFT-join chain.
#
# CONNECTOR CONSTRAINT: flink-sql-connector-kafka has 5.0.0-2.1 / 5.0.0-2.2, but
# flink-sql-connector-elasticsearch7 stops at 4.0.0-2.0 -- there is no 2.1+ build, and the connector
# repo's newest release branch is v4.0. The 2.0 ES jar is therefore carried onto 2.2 on the
# assumption that Flink 2.x keeps the Sink V2 connector API stable across minors. UNVERIFIED: if it
# is wrong the job fails at submission, which is cheap to discover.
ARG FLINK_IMAGE=flink:2.0.2-java17
FROM ${FLINK_IMAGE}

# artifact version = <connector-ver>-<flink-minor>
ARG MAVEN=https://repo1.maven.org/maven2/org/apache/flink
ARG KAFKA_CONNECTOR=flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar
ARG ES7_CONNECTOR=flink-sql-connector-elasticsearch7/4.0.0-2.0/flink-sql-connector-elasticsearch7-4.0.0-2.0.jar

USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*; \
    cd /opt/flink/lib; \
    curl -fsSLO "${MAVEN}/${KAFKA_CONNECTOR}"; \
    curl -fsSLO "${MAVEN}/${ES7_CONNECTOR}"; \
    # flink-json (provides the debezium-json format) ships in /opt/flink/opt in the dist -- make sure
    # it is on the runtime classpath for SQL. Harmless no-op if already present in lib.
    if ls /opt/flink/opt/flink-json*.jar >/dev/null 2>&1; then cp /opt/flink/opt/flink-json*.jar /opt/flink/lib/ || true; fi; \
    ls -l /opt/flink/lib/flink-sql-connector-*.jar
USER flink

# The SQL job + helper scripts (debezium/flink/) are mounted at runtime, not baked, so they can be
# iterated without rebuilding the image.
