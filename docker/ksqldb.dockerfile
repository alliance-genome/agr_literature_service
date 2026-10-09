# Custom ksqlDB server image (SCRUM-6231): bakes in a RocksDB config-setter so ksqlDB's RocksDB
# state stores use compression + universal compaction + a shared background-I/O rate limiter --
# the disk-write-VOLUME levers that are NOT reachable via env alone. The reindex is throughput-bound
# at the m5.4xlarge ~594 MB/s instance EBS ceiling and env-only throttling was exhausted (see
# docs/superpowers/specs/2026-06-25-ksqldb-rocksdb-config-setter-design.md and
# reindex-report-threads2-2026-06-25.md).
#
# Builds on both amd64 and arm64 (SCRUM-6663, Graviton migration). confluentinc/ksqldb-server is
# published for amd64 only (0.26.0 through latest), but ksqlDB is pure Java and its
# rocksdbjni-6.29.4.1 jar already ships librocksdbjni-linux-aarch64.so. So the image lifts the
# unchanged ksqlDB 0.26.0 jars, scripts and config templates out of the amd64 image (COPY only,
# nothing amd64 is executed) onto cp-base-new:7.2.0, the first Confluent base published for arm64.
# It has the same UBI8 + Zulu OpenJDK 11 + dub/cub layout as the 7.0.1 base the stock image uses,
# and 7.2.0 matches the kafka-streams 7.2.0-ccs jars ksqlDB 0.26.0 bundles.
FROM --platform=linux/amd64 confluentinc/ksqldb-server:0.26.0 AS upstream

# --- build stage: compile the config-setter against ksqlDB 0.26.0's OWN kafka-streams + rocksdbjni
#     jars (kafka-streams 7.2.0-ccs / Kafka 3.2, rocksdbjni 6.29.4.1), so the compiled API matches
#     the runtime exactly -- no version guessing. The stock image is JRE-only, hence a JDK build stage.
FROM eclipse-temurin:11-jdk AS build
# The ksqlDB classpath dir holds kafka-streams-*, rocksdbjni-*, kafka-clients-* and all transitive
# deps; copying the whole dir makes the compile robust to the jars' build-suffix version strings.
COPY --from=upstream /usr/share/java/ksqldb-rest-app/ /libs/
COPY docker/ksqldb/KsqlRocksDBConfigSetter.java /src/KsqlRocksDBConfigSetter.java
RUN javac --release 11 -cp "/libs/*" -d /out /src/KsqlRocksDBConfigSetter.java \
 && jar cf /ksql-rocksdb-config-setter.jar -C /out .

# --- final stage: stock ksqlDB 0.26.0 on a multi-arch Confluent base, plus our jar ---
FROM confluentinc/cp-base-new:7.2.0
USER root
COPY --from=upstream --chown=appuser:appuser /usr/share/java/ksqldb-rest-app/ /usr/share/java/ksqldb-rest-app/
COPY --from=upstream --chown=appuser:appuser /usr/bin/ksql* /usr/bin/
COPY --from=upstream --chown=appuser:appuser /usr/bin/docker/ /usr/bin/docker/
COPY --from=upstream --chown=appuser:appuser /etc/ksqldb/ /etc/ksqldb/
# Drop the jar into the dir the ksqlDB JVM loads (/usr/share/java/ksqldb-rest-app/*), putting
# org.alliancegenome.ksql.KsqlRocksDBConfigSetter on the classpath.
COPY --from=build /ksql-rocksdb-config-setter.jar \
     /usr/share/java/ksqldb-rest-app/ksql-rocksdb-config-setter.jar
RUN chmod +x /usr/bin/docker/run /usr/bin/docker/configure /usr/bin/ksql*
USER appuser
WORKDIR /home/appuser
CMD ["/usr/bin/docker/run"]
