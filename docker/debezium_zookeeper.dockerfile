ARG DEBEZIUM_VERSION=2.7
FROM quay.io/debezium/zookeeper:${DEBEZIUM_VERSION}

# zkServer.sh prints two routine startup lines on stderr, which the GELF driver files as
# ERROR on every Debezium rebuild. Send them to stdout instead. ZooKeeper's own logging
# already goes to stdout (logback's CONSOLE appender), so stderr is left for real failures.
# The grep fails the build if a new image changes either line, rather than letting the
# ERRORs come back unnoticed.
RUN sed -i \
        -e 's|^\( *echo "ZooKeeper JMX enabled by default"\) >&2$|\1|' \
        -e 's|^\( *echo "Using config: $ZOOCFG"\) >&2$|\1|' \
        $ZK_HOME/bin/zkServer.sh && \
    grep -q '^ *echo "ZooKeeper JMX enabled by default"$' $ZK_HOME/bin/zkServer.sh && \
    grep -q '^ *echo "Using config: $ZOOCFG"$' $ZK_HOME/bin/zkServer.sh
