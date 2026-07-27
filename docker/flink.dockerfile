# SCRUM-6336: Flink 2.0.x image with the SQL connector jars baked onto the classpath.
# Flink 2.0.2 is the ceiling supported by the Elasticsearch-7 connector (v4.0.0-2.0); the Kafka
# connector (4.0.1-2.0) pairs with the same Flink minor. Both coordinates verified present on
# Maven Central (HTTP 200) before pinning.
FROM flink:2.0.2-java17

# Registry-verified connector coordinates (artifact version = <connector-ver>-<flink-minor>):
#   flink-sql-connector-kafka           4.0.1-2.0  -> Debezium CDC source (debezium-json format)
#   flink-sql-connector-elasticsearch7  4.0.0-2.0  -> ES-7 upsert sink -> Elasticsearch 7.10
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
