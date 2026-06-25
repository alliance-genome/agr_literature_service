# Custom ksqlDB server image (SCRUM-6231): bakes in a RocksDB config-setter so ksqlDB's RocksDB
# state stores use compression + universal compaction + a shared background-I/O rate limiter --
# the disk-write-VOLUME levers that are NOT reachable via env alone. The reindex is throughput-bound
# at the m5.4xlarge ~594 MB/s instance EBS ceiling and env-only throttling was exhausted (see
# docs/superpowers/specs/2026-06-25-ksqldb-rocksdb-config-setter-design.md and
# reindex-report-threads2-2026-06-25.md).

# --- build stage: compile the config-setter against ksqlDB 0.26.0's OWN kafka-streams + rocksdbjni
#     jars (kafka-streams 7.2.0-ccs / Kafka 3.2, rocksdbjni 6.29.4.1), so the compiled API matches
#     the runtime exactly -- no version guessing. The stock image is JRE-only, hence a JDK build stage.
FROM eclipse-temurin:11-jdk AS build
# The ksqlDB classpath dir holds kafka-streams-*, rocksdbjni-*, kafka-clients-* and all transitive
# deps; copying the whole dir makes the compile robust to the jars' build-suffix version strings.
COPY --from=confluentinc/ksqldb-server:0.26.0 /usr/share/java/ksqldb-rest-app/ /libs/
COPY docker/ksqldb/KsqlRocksDBConfigSetter.java /src/KsqlRocksDBConfigSetter.java
RUN javac --release 11 -cp "/libs/*" -d /out /src/KsqlRocksDBConfigSetter.java \
 && jar cf /ksql-rocksdb-config-setter.jar -C /out .

# --- final stage: the stock ksqlDB server + our jar on its classpath ---
FROM confluentinc/ksqldb-server:0.26.0
# Drop the jar into the dir the ksqlDB JVM loads (/usr/share/java/ksqldb-rest-app/*), putting
# org.alliancegenome.ksql.KsqlRocksDBConfigSetter on the classpath.
COPY --from=build /ksql-rocksdb-config-setter.jar \
     /usr/share/java/ksqldb-rest-app/ksql-rocksdb-config-setter.jar
