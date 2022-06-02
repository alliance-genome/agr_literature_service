#!/bin/bash
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/references_index -d @/elasticsearch-settings.json
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst < /postgres-source.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT' < /elasticsearch-sink.json)
