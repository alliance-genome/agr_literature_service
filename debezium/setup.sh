#!/bin/bash
curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME}
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME} -d @/elasticsearch-settings.json
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-reference.json)
sleep 10
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-joined_tables.json)
sleep 120

curl -X "POST" http://${DEBEZIUM_KSQLDB_HOST}:${DEBEZIUM_KSQLDB_PORT}/ksql -H "Accept: application/vnd.ksql.v1+json" -H "Content-Type: application/json" -d @/ksql_queries.ksql
sleep 10

curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$DEBEZIUM_INDEX_NAME' < /elasticsearch-sink.json)
