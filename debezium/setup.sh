#!/bin/bash
DEBEZIUM_INDEX_NAME_ORIG="${DEBEZIUM_INDEX_NAME}"
DEBEZIUM_INDEX_NAME="${DEBEZIUM_INDEX_NAME}_temp"
curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME}
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME} -d @/elasticsearch-settings.json
export PGPASSWORD=${PSQL_PASSWORD}
psql -h ${PSQL_HOST} -U ${PSQL_USERNAME} -p ${PSQL_PORT} -d ${PSQL_DATABASE} -c "select pg_drop_replication_slot('debezium_mod'); select pg_drop_replication_slot('debezium_referencetype'); select pg_drop_replication_slot('debezium_reference'); select pg_drop_replication_slot('debezium_joined_tables'); select pg_drop_replication_slot('debezium_citation'); select pg_drop_replication_slot('debezium_mod_referencetype'); select pg_drop_replication_slot('debezium_topic_entity_tag_source');"
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-reference.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-citation.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-mod.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-mod-referencetype.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-referencetype.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-topic-entity-tag-source.json)
sleep 10
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-joined_tables.json)
sleep 300

curl -X "POST" http://${DEBEZIUM_KSQLDB_HOST}:${DEBEZIUM_KSQLDB_PORT}/ksql -H "Accept: application/vnd.ksql.v1+json" -H "Content-Type: application/json" -d @/ksql_queries.ksql
sleep 10

export SINK_NAME="elastic-sink-temp"
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$DEBEZIUM_INDEX_NAME$SINK_NAME' < /elasticsearch-sink.json)

sleep 7200
new_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME}/_count | jq '.count')
if [[ $new_index_doc_count -gt 0 ]]
then
  curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/elastic-sink
  DEBEZIUM_INDEX_NAME="${DEBEZIUM_INDEX_NAME_ORIG}"
  curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME_ORIG}
  curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME_ORIG} -d @/elasticsearch-settings.json
  curl -i -X POST -H "Accept:application/json" -H "Content-Type: application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/_reindex?pretty -d"{\"source\": {\"index\": \"${DEBEZIUM_INDEX_NAME}\"}, \"dest\": {\"index\": \"${DEBEZIUM_INDEX_NAME_ORIG}\"}}"
  export SINK_NAME="elastic-sink"
  curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$DEBEZIUM_INDEX_NAME$SINK_NAME' < /elasticsearch-sink.json)
fi
curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/elastic-sink-temp