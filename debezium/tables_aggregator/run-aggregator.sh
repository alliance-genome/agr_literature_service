#!/bin/bash

export PATH="/opt/tables_aggregator/jdk/bin:${PATH}"
export JAVA_APP_DIR=/opt/tables_aggregator/lib
export JAVA_MAIN_CLASS=io.debezium.examples.aggregation.StreamingAggregatesDDDLit

exec /opt/tables_aggregator/run-java.sh "$PARENT_TOPIC" "$CHILDREN_TOPIC" "$BOOTSTRAP_SERVERS"
